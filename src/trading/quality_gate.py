"""Pre-buy quality gate: watch a new coin briefly and buy only if it shows real demand.

The stock bot buys every coin the instant it launches. Most launches are dead or
rugged within a minute, so this gate trades speed for selectivity:

1. cheap name/creator checks the moment the coin is seen,
2. an observation window (``observe_seconds``) sampling the bonding curve twice,
3. holder-concentration and dev-wallet checks at the end of the window.

The coin is bought only if every check passes. ``dry_run`` logs the would-be
trade and paper-tracks its outcome to ``trades/paper_trades.csv`` instead of
spending anything.

Pass/fail logic lives in ``judge_snapshot`` (pure, unit-tested); the class only
gathers the numbers.
"""

import asyncio
import csv
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from time import monotonic, time
from typing import Any

from solders.pubkey import Pubkey
from spl.token.instructions import get_associated_token_address

from core.client import SolanaClient
from core.pubkeys import SystemAddresses, WSOL_MINT, normalize_quote_mint
from interfaces.core import TokenInfo
from utils.logger import get_logger

logger = get_logger(__name__)

LAMPORTS = 1_000_000_000
PUMP_TOTAL_SUPPLY = 1_000_000_000 * 10**6  # pump.fun: 1B tokens, 6 decimals
# pump.fun curves start with 30 SOL of virtual reserves; real SOL raised is
# vSolInBondingCurve minus this.
INITIAL_VIRTUAL_SOL = 30.0


@dataclass
class GateConfig:
    """Thresholds. SOL amounts are whole SOL; percentages are 0..1 fractions."""

    enabled: bool = True
    dry_run: bool = True
    observe_seconds: float = 30.0
    max_concurrent_watches: int = 5
    min_sol_raised: float = 4.0
    max_sol_raised: float = 45.0
    min_sol_growth_second_half: float = 0.5
    min_sol_growth_first_half: float = 0.0
    # Curve transactions in the window, read with one getSignaturesForAddress:
    # a few big buys (dev bundle, one wallet pumping) instead of a crowd.
    # 0 turns the count off; None from the RPC means not measured.
    min_curve_txs: int = 0
    max_avg_sol_per_tx: float = 0.0  # 0 = off
    min_unique_buyers: int = 8  # distinct buying wallets in the window (stream path)
    max_top_holder_pct: float = 0.08
    # getTokenLargestAccounts needs a Chainstack Dedicated Node. When the RPC
    # refuses it, True skips every coin; False trades without the holder check.
    require_holder_check: bool = False
    max_top10_holders_pct: float = 0.30
    max_dev_holding_pct: float = 0.05
    skip_if_dev_sold: bool = True
    max_launches_per_creator: int = 1
    min_symbol_len: int = 2
    max_symbol_len: int = 10
    blocked_words: list[str] = field(
        default_factory=lambda: ["test", "rug", "scam", "honeypot", "airdrop"]
    )
    wallet_floor_sol: float = 0.05
    paper_take_profit: float = 0.5
    paper_stop_loss: float = 0.25
    paper_max_hold: float = 120.0
    paper_check_interval: float = 3.0
    paper_round_trip_cost_pct: float = 0.03

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> "GateConfig":
        """Build from the YAML ``quality_gate`` section; unknown keys raise."""
        d = dict(d or {})
        unknown = set(d) - set(cls.__dataclass_fields__)
        if unknown:
            raise ValueError(f"Unknown quality_gate keys: {sorted(unknown)}")
        return cls(**d)


@dataclass
class Snapshot:
    """What the gate measured about one coin. Fractions are of total supply."""

    sol_start: float
    sol_mid: float
    sol_end: float
    complete: bool
    top_holder_pct: float | None  # None: the RPC cannot answer getTokenLargestAccounts
    top10_pct: float | None
    dev_pct_start: float
    dev_pct_end: float
    unique_buyers: int | None = None  # None: not measured (RPC path)
    curve_txs: int | None = (
        None  # transactions on the curve in the window; None: not measured
    )


def judge_name(symbol: str, name: str, cfg: GateConfig) -> str | None:
    """Reason to skip on name alone, or None."""
    sym = (symbol or "").strip()
    if not cfg.min_symbol_len <= len(sym) <= cfg.max_symbol_len:
        return f"symbol length {len(sym)} outside {cfg.min_symbol_len}-{cfg.max_symbol_len}"
    text = f"{sym} {name or ''}".lower()
    for word in cfg.blocked_words:
        if word.lower() in text:
            return f"blocked word '{word}'"
    return None


def judge_snapshot(s: Snapshot, cfg: GateConfig) -> str | None:
    """Reason to skip after the observation window, or None to buy."""
    if s.complete:
        return "curve already complete"
    if s.sol_end < cfg.min_sol_raised:
        return f"only {s.sol_end:.2f} SOL raised (< {cfg.min_sol_raised})"
    if s.sol_end > cfg.max_sol_raised:
        return f"{s.sol_end:.2f} SOL raised - too late (> {cfg.max_sol_raised})"
    if s.sol_end - s.sol_mid < cfg.min_sol_growth_second_half:
        return (
            f"momentum fading: +{s.sol_end - s.sol_mid:.2f} SOL in second half "
            f"(< {cfg.min_sol_growth_second_half})"
        )
    if s.sol_mid - s.sol_start < cfg.min_sol_growth_first_half:
        return (
            f"slow start: +{s.sol_mid - s.sol_start:.2f} SOL in first half "
            f"(< {cfg.min_sol_growth_first_half})"
        )
    if s.curve_txs is not None:
        if cfg.min_curve_txs and s.curve_txs < cfg.min_curve_txs:
            return f"only {s.curve_txs} curve transactions (< {cfg.min_curve_txs})"
        raised = s.sol_end - s.sol_start
        if (
            cfg.max_avg_sol_per_tx
            and s.curve_txs
            and raised / s.curve_txs > cfg.max_avg_sol_per_tx
        ):
            return (
                f"{raised / s.curve_txs:.2f} SOL per transaction - a few big buyers "
                f"(> {cfg.max_avg_sol_per_tx})"
            )
    if s.unique_buyers is not None and s.unique_buyers < cfg.min_unique_buyers:
        return f"only {s.unique_buyers} distinct buyers (< {cfg.min_unique_buyers})"
    if s.top_holder_pct is None or s.top10_pct is None:
        if cfg.require_holder_check:
            return "holder data unavailable from this RPC"
    else:
        if s.top_holder_pct > cfg.max_top_holder_pct:
            return f"top holder owns {s.top_holder_pct:.1%} (> {cfg.max_top_holder_pct:.0%})"
        if s.top10_pct > cfg.max_top10_holders_pct:
            return f"top 10 holders own {s.top10_pct:.1%} (> {cfg.max_top10_holders_pct:.0%})"
    if s.dev_pct_end > cfg.max_dev_holding_pct:
        return f"dev holds {s.dev_pct_end:.1%} (> {cfg.max_dev_holding_pct:.0%})"
    if cfg.skip_if_dev_sold and s.dev_pct_end < s.dev_pct_start - 1e-6:
        return f"dev sold during window ({s.dev_pct_start:.1%} -> {s.dev_pct_end:.1%})"
    return None


def _pct(x: float | None) -> str:
    return "n/a" if x is None else f"{x:.1%}"


class QualityGate:
    """Gathers on-chain numbers for a coin and applies the judges above."""

    def __init__(self, cfg: GateConfig, client: SolanaClient, curve_manager: Any):
        self.cfg = cfg
        self.client = client
        self.curve_manager = curve_manager
        self.creator_launches: dict[str, set[str]] = {}
        self.stats = {"seen": 0, "passed": 0}
        self.holders_unavailable = False
        self.signatures_unavailable = False

    def note_launch(self, token_info: TokenInfo) -> None:
        """Count launches per creator; call for every coin detected, bought or not."""
        # Counted per distinct mint: the listener often reports one coin twice.
        if token_info.creator:
            key = str(token_info.creator)
            self.creator_launches.setdefault(key, set()).add(str(token_info.mint))

    def quick_reject(self, token_info: TokenInfo) -> str | None:
        """Checks that need no RPC call."""
        if normalize_quote_mint(token_info.quote_mint) != WSOL_MINT:
            return "not SOL-paired"
        if token_info.is_mayhem_mode:
            return "mayhem-mode coin"
        launches = len(self.creator_launches.get(str(token_info.creator), ()))
        if launches > self.cfg.max_launches_per_creator:
            return f"creator launched {launches} coins this session (serial launcher)"
        return judge_name(token_info.symbol, token_info.name, self.cfg)

    async def wallet_below_floor(self, owner: Pubkey) -> bool:
        resp = await self.client.post_rpc(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [str(owner), {"commitment": "confirmed"}],
            }
        )
        if not resp or "result" not in resp:
            logger.warning("Could not read wallet balance; refusing to buy")
            return True
        sol = resp["result"]["value"] / LAMPORTS
        if sol < self.cfg.wallet_floor_sol:
            logger.warning(
                f"Wallet {sol:.4f} SOL is below wallet_floor_sol {self.cfg.wallet_floor_sol}"
            )
            return True
        return False

    async def _sol_raised(self, pool: Pubkey) -> tuple[float, bool]:
        # "processed": the curve of a coin seconds old may not be visible at confirmed yet
        state = await self.curve_manager.get_pool_state(pool, commitment="processed")
        return state["real_sol_reserves"] / LAMPORTS, bool(state.get("complete"))

    async def _holders(
        self, token_info: TokenInfo
    ) -> tuple[float | None, float | None]:
        """Top holder and top-10 share of supply, excluding the bonding curve's vault.

        Returns (None, None) once the RPC has refused the method; it is not asked again.
        """
        if self.holders_unavailable:
            return None, None
        resp = await self.client.post_rpc(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenLargestAccounts",
                "params": [str(token_info.mint), {"commitment": "confirmed"}],
            },
            max_retries=1,
            deadline_seconds=5,
        )
        if not resp or "result" not in resp:
            self.holders_unavailable = True
            logger.warning(
                "[gate] RPC refused getTokenLargestAccounts (Chainstack offers it on "
                "Dedicated Nodes only) - holder-concentration check is off"
            )
            return None, None
        curve_vault = str(token_info.associated_bonding_curve or "")
        amounts = [
            int(a["amount"])
            for a in resp["result"]["value"]
            if a["address"] != curve_vault
        ]
        amounts.sort(reverse=True)
        top = amounts[0] / PUMP_TOTAL_SUPPLY if amounts else 0.0
        return top, sum(amounts[:10]) / PUMP_TOTAL_SUPPLY

    async def _curve_tx_count(self, pool: Pubkey, since: float) -> int | None:
        """Transactions touching the bonding curve since ``since`` (unix seconds).

        None when the RPC refuses getSignaturesForAddress; it is not asked again.
        Failed transactions are not counted. Capped at 1000 (one page).
        """
        if self.signatures_unavailable or not (
            self.cfg.min_curve_txs or self.cfg.max_avg_sol_per_tx
        ):
            return None
        resp = await self.client.post_rpc(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [str(pool), {"limit": 1000, "commitment": "confirmed"}],
            },
            max_retries=1,
            deadline_seconds=5,
        )
        if not resp or "result" not in resp:
            self.signatures_unavailable = True
            logger.warning(
                "[gate] RPC refused getSignaturesForAddress - transaction-count check is off"
            )
            return None
        return sum(
            1
            for r in resp["result"]
            if r.get("err") is None
            and (r.get("blockTime") is None or r["blockTime"] >= since)
        )

    async def _dev_pct(self, token_info: TokenInfo) -> float:
        """Creator's share of supply, read from their associated token account.

        Reads one account with getAccountInfo, which every RPC plan serves;
        getTokenAccountsByOwner is paid-only on Chainstack.
        """
        if not token_info.creator:
            return 0.0
        program = token_info.token_program_id or SystemAddresses.TOKEN_2022_PROGRAM
        ata = get_associated_token_address(token_info.creator, token_info.mint, program)
        resp = await self.client.post_rpc(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [
                    str(ata),
                    {"encoding": "jsonParsed", "commitment": "processed"},
                ],
            },
            deadline_seconds=5,
        )
        if not resp or "result" not in resp:
            raise ValueError("getAccountInfo failed for dev token account")
        value = resp["result"]["value"]
        if value is None:
            return 0.0  # no token account: the creator holds none
        amount = value["data"]["parsed"]["info"]["tokenAmount"]["amount"]
        return int(amount) / PUMP_TOTAL_SUPPLY

    async def evaluate(
        self, token_info: TokenInfo, pool: Pubkey, launch: Any | None = None
    ) -> str | None:
        """Watch the coin for ``observe_seconds`` via RPC; reason to skip, or None to buy.

        ``launch`` (a PumpPortal create: v_sol, initial_buy_tokens) supplies the
        starting SOL and dev holding for free. A coin that fails on SOL raised
        costs two curve reads; the dev and holder reads happen only after that.
        """
        self.stats["seen"] += 1
        reason = self.quick_reject(token_info)
        if reason:
            return reason
        started = time() - 5  # the coin launched a few seconds before we saw it
        try:
            if launch is not None:
                sol_start = max(0.0, launch.v_sol - INITIAL_VIRTUAL_SOL)
                dev_start = launch.initial_buy_tokens / (PUMP_TOTAL_SUPPLY / 10**6)
            else:
                sol_start, _ = await self._sol_raised(pool)
                dev_start = await self._dev_pct(token_info)
            await asyncio.sleep(self.cfg.observe_seconds / 2)
            sol_mid, done = await self._sol_raised(pool)
            if done:
                return "curve completed during window"
            await asyncio.sleep(self.cfg.observe_seconds / 2)
            sol_end, done = await self._sol_raised(pool)
            early = Snapshot(
                sol_start, sol_mid, sol_end, done, None, None, dev_start, dev_start
            )
            reason = judge_snapshot(
                early, replace(self.cfg, require_holder_check=False)
            )
            if reason:
                logger.info(
                    f"[gate] {token_info.symbol}: SOL {sol_start:.2f}->{sol_mid:.2f}->{sol_end:.2f}"
                )
                return reason
            curve_txs = await self._curve_tx_count(pool, started)
            top, top10 = await self._holders(token_info)
            dev_end = await self._dev_pct(token_info)
        except Exception as e:  # noqa: BLE001 - any read failure means "don't buy"
            return f"could not measure coin ({e})"
        snap = Snapshot(
            sol_start,
            sol_mid,
            sol_end,
            done,
            top,
            top10,
            dev_start,
            dev_end,
            curve_txs=curve_txs,
        )
        logger.info(
            f"[gate] {token_info.symbol}: SOL {sol_start:.2f}->{sol_mid:.2f}->{sol_end:.2f}, "
            f"txs {curve_txs if curve_txs is not None else 'n/a'}, "
            f"top {_pct(top)}, top10 {_pct(top10)}, dev {dev_start:.1%}->{dev_end:.1%}"
        )
        reason = judge_snapshot(snap, self.cfg)
        if reason is None:
            self.stats["passed"] += 1
        logger.info(
            f"[gate] passed {self.stats['passed']} of {self.stats['seen']} coins evaluated"
        )
        return reason

    async def evaluate_stream(self, token_info: TokenInfo, stream: Any) -> str | None:
        """Watch the coin through a trade stream; reason to skip, or None to buy.

        ``stream`` is a PumpPortalListener: ``launches`` holds the create
        message, ``watch``/``unwatch`` collect trades. No RPC is used unless
        the coin passes everything else, then one holder lookup.
        """
        self.stats["seen"] += 1
        reason = self.quick_reject(token_info)
        if reason:
            return reason
        mint = str(token_info.mint)
        launch = stream.launches.get(mint)
        if launch is None:
            return "no launch data from the stream"
        initial_v = INITIAL_VIRTUAL_SOL
        watch = await stream.watch(mint)
        try:
            await asyncio.sleep(self.cfg.observe_seconds / 2)
            v_mid = watch.last_v_sol or launch.v_sol
            await asyncio.sleep(self.cfg.observe_seconds / 2)
            v_end = watch.last_v_sol or v_mid
        finally:
            await stream.unwatch(mint)

        creator = launch.creator
        buyers = {
            e.get("traderPublicKey") for e in watch.events if e.get("txType") == "buy"
        }
        buyers.discard(creator)
        dev_tokens_end = launch.initial_buy_tokens
        for e in watch.events:
            if e.get("traderPublicKey") == creator and isinstance(
                e.get("newTokenBalance"), (int, float)
            ):
                dev_tokens_end = float(e["newTokenBalance"])
        dev_sold = any(
            e.get("traderPublicKey") == creator and e.get("txType") == "sell"
            for e in watch.events
        )
        supply_tokens = PUMP_TOTAL_SUPPLY / 10**6
        dev_start = launch.initial_buy_tokens / supply_tokens
        dev_end = dev_tokens_end / supply_tokens
        if dev_sold and dev_end >= dev_start:
            dev_end = dev_start - 1e-6  # a dev sell counts even if they rebought

        snap = Snapshot(
            sol_start=max(0.0, launch.v_sol - initial_v),
            sol_mid=max(0.0, v_mid - initial_v),
            sol_end=max(0.0, v_end - initial_v),
            complete=False,
            top_holder_pct=None,
            top10_pct=None,
            dev_pct_start=dev_start,
            dev_pct_end=dev_end,
            unique_buyers=len(buyers),
        )
        reason = judge_snapshot(snap, self.cfg)
        if reason is None and not self.holders_unavailable:
            try:
                snap.top_holder_pct, snap.top10_pct = await self._holders(token_info)
            except Exception as e:  # noqa: BLE001
                return f"could not read holders ({e})"
            reason = judge_snapshot(snap, self.cfg)
        logger.info(
            f"[gate] {token_info.symbol}: SOL {snap.sol_start:.2f}->{snap.sol_mid:.2f}->"
            f"{snap.sol_end:.2f}, buyers {snap.unique_buyers}, trades {len(watch.events)}, "
            f"top {_pct(snap.top_holder_pct)}, dev {dev_start:.1%}->{dev_end:.1%}"
        )
        if reason is None:
            self.stats["passed"] += 1
        logger.info(
            f"[gate] passed {self.stats['passed']} of {self.stats['seen']} coins evaluated"
        )
        return reason

    async def paper_trade(self, token_info: TokenInfo, read_price) -> None:
        """Track a would-be buy with the configured TP/SL and append the result to CSV."""
        cfg = self.cfg
        try:
            entry = await read_price(token_info)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[paper] no entry price for {token_info.symbol}: {e}")
            return
        if not entry or entry <= 0:
            return
        start, outcome, last = monotonic(), "timeout", entry
        while monotonic() - start < cfg.paper_max_hold:
            await asyncio.sleep(cfg.paper_check_interval)
            try:
                price = await read_price(token_info)
            except Exception:  # noqa: BLE001
                continue
            if not price or price <= 0:
                continue
            last = price
            change = price / entry - 1
            if change >= cfg.paper_take_profit:
                outcome = "take_profit"
                break
            if change <= -cfg.paper_stop_loss:
                outcome = "stop_loss"
                break
        pnl = last / entry - 1 - cfg.paper_round_trip_cost_pct
        logger.info(
            f"[paper] {token_info.symbol}: {outcome}, net {pnl:+.1%} after costs"
        )
        path = Path("trades") / "paper_trades.csv"
        path.parent.mkdir(exist_ok=True)
        new = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(
                    [
                        "time",
                        "symbol",
                        "mint",
                        "entry",
                        "exit",
                        "outcome",
                        "net_pnl_pct",
                    ]
                )
            w.writerow(
                [
                    datetime.now().isoformat(timespec="seconds"),
                    token_info.symbol,
                    str(token_info.mint),
                    f"{entry:.10f}",
                    f"{last:.10f}",
                    outcome,
                    f"{pnl * 100:.1f}",
                ]
            )
