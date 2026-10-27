"""pump.fun launches and trades from PumpPortal's free data stream.

One WebSocket to wss://pumpportal.fun/api/data carries both:

- ``subscribeNewToken``: every pump.fun create, a few per second. This replaces
  ``logsSubscribe`` on the RPC, which streams every pump.fun transaction and
  burns an RPC plan's monthly request allowance within hours.
- ``subscribeTokenTrade``: buys and sells of the mints in ``keys``, opened per
  coin with ``watch()`` while the quality gate observes it.

PumpPortal asks clients to keep a single connection and add keys to it rather
than open one connection per subscription.

The TokenInfo built here carries the create's submitter as ``creator`` and
guesses Token-2022; ``state_from_event`` stays False, so the trader must refresh
creator, flags, quote mint and token program from the curve before buying.
"""

import asyncio
import json
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from time import monotonic

import websockets
from solders.pubkey import Pubkey

from core.pubkeys import SystemAddresses
from interfaces.core import Platform, TokenInfo
from monitoring.base_listener import BaseTokenListener, reraise_if_cancelled
from platforms.pumpfun.address_provider import PumpFunAddressProvider
from utils.logger import get_logger

logger = get_logger(__name__)

PUMPPORTAL_WSS = "wss://pumpportal.fun/api/data"
# pump.fun curves start with 30 SOL of virtual reserves; real SOL raised is
# vSolInBondingCurve minus this.
INITIAL_VIRTUAL_SOL = 30.0
MAX_REMEMBERED_LAUNCHES = 5000


@dataclass
class Launch:
    """What the create message said about a coin. Token amounts are whole tokens."""

    creator: str
    initial_buy_tokens: float
    v_sol: float


@dataclass
class TradeWatch:
    """Trades of one mint received since ``watch()``; newest last."""

    mint: str
    events: list[dict] = field(default_factory=list)

    @property
    def last_v_sol(self) -> float | None:
        for e in reversed(self.events):
            v = e.get("vSolInBondingCurve")
            if isinstance(v, (int, float)):
                return float(v)
        return None


class PumpPortalListener(BaseTokenListener):
    """New pump.fun coins and per-coin trades from PumpPortal (free, no key)."""

    def __init__(self, url: str = PUMPPORTAL_WSS):
        super().__init__(Platform.PUMP_FUN)
        self.url = url
        self.address_provider = PumpFunAddressProvider()
        self.launches: OrderedDict[str, Launch] = OrderedDict()
        self._watches: dict[str, TradeWatch] = {}
        self._ws = None
        self._send_lock = asyncio.Lock()

    async def _send(self, payload: dict) -> None:
        ws = self._ws
        if ws is None:
            return  # resubscribed on reconnect
        async with self._send_lock:
            try:
                await ws.send(json.dumps(payload))
            except websockets.exceptions.ConnectionClosed:
                pass  # the reconnect loop resubscribes every watched key

    async def watch(self, mint: Pubkey | str) -> TradeWatch:
        """Start collecting trades for ``mint``; call ``unwatch`` when done."""
        key = str(mint)
        watch = self._watches.get(key)
        if watch is None:
            watch = self._watches[key] = TradeWatch(key)
            await self._send({"method": "subscribeTokenTrade", "keys": [key]})
        return watch

    async def unwatch(self, mint: Pubkey | str) -> None:
        key = str(mint)
        if self._watches.pop(key, None) is not None:
            await self._send({"method": "unsubscribeTokenTrade", "keys": [key]})

    def _remember(self, mint: str, launch: Launch) -> None:
        self.launches[mint] = launch
        while len(self.launches) > MAX_REMEMBERED_LAUNCHES:
            self.launches.popitem(last=False)

    def _token_info(self, msg: dict) -> TokenInfo | None:
        try:
            mint = Pubkey.from_string(msg["mint"])
            creator = Pubkey.from_string(msg["traderPublicKey"])
            bonding_curve = (
                Pubkey.from_string(msg["bondingCurveKey"])
                if msg.get("bondingCurveKey")
                else self.address_provider.derive_pool_address(mint)
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"[pumpportal] unreadable create message ({e})")
            return None
        token_program = SystemAddresses.TOKEN_2022_PROGRAM
        return TokenInfo(
            name=str(msg.get("name", "")),
            symbol=str(msg.get("symbol", "")),
            uri=str(msg.get("uri", "")),
            mint=mint,
            platform=Platform.PUMP_FUN,
            bonding_curve=bonding_curve,
            associated_bonding_curve=self.address_provider.derive_associated_bonding_curve(
                mint, bonding_curve, token_program
            ),
            user=creator,
            creator=creator,
            creator_vault=self.address_provider.derive_creator_vault(creator),
            token_program_id=token_program,
            state_from_event=False,
            creation_timestamp=monotonic(),
        )

    async def listen_for_tokens(
        self,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None = None,
        creator_address: str | None = None,
    ) -> None:
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=20) as ws:
                    self._ws = ws
                    await self._send({"method": "subscribeNewToken"})
                    if self._watches:
                        await self._send(
                            {"method": "subscribeTokenTrade", "keys": list(self._watches)}
                        )
                    logger.info("[pumpportal] connected; streaming new pump.fun coins")
                    try:
                        async for raw in ws:
                            await self._dispatch(
                                raw, token_callback, match_string, creator_address
                            )
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("[pumpportal] connection closed; reconnecting")
                    finally:
                        self._ws = None
            except Exception:
                reraise_if_cancelled()
                logger.exception("[pumpportal] connection error")
            await asyncio.sleep(3)

    async def _dispatch(
        self,
        raw: str | bytes,
        token_callback: Callable[[TokenInfo], Awaitable[None]],
        match_string: str | None,
        creator_address: str | None,
    ) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            return
        if not isinstance(msg, dict):
            return
        tx_type = msg.get("txType")
        mint = msg.get("mint")
        if tx_type in ("buy", "sell") and mint in self._watches:
            self._watches[mint].events.append(msg)
            return
        if tx_type != "create" or msg.get("pool", "pump") != "pump":
            return  # subscription acks, other launchpads

        def _num(key: str) -> float:
            v = msg.get(key)
            return float(v) if isinstance(v, (int, float)) else 0.0

        self._remember(
            str(mint),
            Launch(str(msg.get("traderPublicKey", "")), _num("initialBuy"),
                   _num("vSolInBondingCurve")),
        )
        token_info = self._token_info(msg)
        if token_info is None:
            return
        if match_string and not (
            match_string.lower() in token_info.name.lower()
            or match_string.lower() in token_info.symbol.lower()
        ):
            return
        if creator_address and str(token_info.creator) != creator_address:
            return
        await token_callback(token_info)
