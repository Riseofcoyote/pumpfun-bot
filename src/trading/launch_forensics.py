"""Launch-block and wallet-funding checks for the quality gate.

Two ideas, rebuilt from gem-search's Bundle Index and Bundle Crews:

* **Launch block.** Buys that land in the same slot as the coin's create
  transaction are a Jito bundle or the fastest snipers. The dev's own first buy
  is inside the create transaction and is not counted.
* **Funding.** The dev and the launch-block buyers are traced back to the wallet
  that last sent each of them real SOL. Several of them sharing one funder (or
  the dev funding a buyer) is one person on several wallets. Funders are
  remembered across coins in ``trades/crews.json`` so a crew is recognised on
  its next launch even behind a fresh creator wallet.

A shared funder is a funding pattern on chain, not proof of identity. Exchange
hot wallets and any funder seen topping up many wallets are services and never
count.
"""

import asyncio
import json
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from utils.logger import get_logger

logger = get_logger(__name__)

LAMPORTS = 1_000_000_000
MIN_FUNDING_LAMPORTS = 10_000_000  # 0.01 SOL: smaller transfers are dust, not funding
SIGNATURE_PAGE = 1000  # getSignaturesForAddress maximum per call
SYSTEM_PROGRAM = "11111111111111111111111111111111"

# Exchange hot wallets fund thousands of strangers; a shared exchange funder links nobody.
EXCHANGES = frozenset(
    {
        "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9",  # Binance
        "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",  # Binance
        "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S",  # Binance
        "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS",  # Coinbase
        "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE",  # Coinbase
        "2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm",  # Coinbase
        "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2",  # Bybit
        "5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD",  # OKX
        "is6MTRHEgyFLNTfYcuV4QBWLjrZBfmhVNYR6ccgr8KV",  # OKX
        "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5",  # Kraken
        "BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6",  # KuCoin
        "G2YxRa6wt1qePMwfJzdXZG62ej4qaTC7YURzuh2Lwd3t",  # MEXC
        "ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ",  # MEXC
        "u6PJ8DtQuPFnfmwHbGFULQ4u4EgjDiyYKjVEsynXq2w",  # Gate
    }
)

# SolanaClient.post_rpc
PostRpc = Callable[..., Awaitable[dict[str, Any] | None]]


@dataclass
class LaunchBlock:
    """The create transaction's slot and the other successful transactions in it."""

    slot: int
    create_signature: str
    others: list[str] = field(default_factory=list)

    @property
    def buys(self) -> int:
        return len(self.others)


def find_launch_block(
    signatures: list[dict[str, Any]], create_signature: str = ""
) -> LaunchBlock | None:
    """Locate the launch block in a getSignaturesForAddress reply (newest first).

    With ``create_signature`` its slot is used. Without it, or when it is not in
    the list, the oldest entry stands in for the create, but only when the list
    is shorter than one full page (1000): a full page may not reach the start,
    and guessing then would understate a bundle. None means unknown.
    """
    create = None
    if create_signature:
        create = next(
            (s for s in signatures if s.get("signature") == create_signature), None
        )
    if create is None and signatures and len(signatures) < SIGNATURE_PAGE:
        create = signatures[-1]
    if create is None or create.get("slot") is None:
        return None
    slot, sig = create["slot"], create["signature"]
    others = [
        s["signature"]
        for s in signatures
        if s.get("slot") == slot and s.get("signature") != sig and s.get("err") is None
    ]
    return LaunchBlock(slot, sig, others)


def _all_instructions(tx: dict[str, Any]) -> list[dict[str, Any]]:
    outer = tx.get("transaction", {}).get("message", {}).get("instructions", []) or []
    inner = [
        ix
        for group in (tx.get("meta") or {}).get("innerInstructions", []) or []
        for ix in group.get("instructions", []) or []
    ]
    return list(outer) + inner


def fee_payer(tx: dict[str, Any] | None) -> str | None:
    """The first signer of a jsonParsed getTransaction result, or None."""
    if not tx:
        return None
    for key in (
        tx.get("transaction", {}).get("message", {}).get("accountKeys", []) or []
    ):
        if isinstance(key, dict) and key.get("signer"):
            return key.get("pubkey")
    return None


def funding_transfer(
    tx: dict[str, Any] | None, wallet: str
) -> tuple[str, float] | None:
    """(source, SOL) of a system transfer of at least 0.01 SOL into ``wallet``, or None.

    Inner instructions count, so a transfer routed through a program is found too.
    A failed transaction moved nothing and returns None.
    """
    if not tx or (tx.get("meta") or {}).get("err") is not None:
        return None
    for ix in _all_instructions(tx):
        parsed = ix.get("parsed")
        if not isinstance(parsed, dict):
            continue
        if ix.get("program") != "system" and ix.get("programId") != SYSTEM_PROGRAM:
            continue
        if parsed.get("type") not in ("transfer", "transferWithSeed"):
            continue
        info = parsed.get("info") or {}
        lamports = int(info.get("lamports") or 0)
        if info.get("destination") == wallet and lamports >= MIN_FUNDING_LAMPORTS:
            return info.get("source"), lamports / LAMPORTS
    return None


class CrewMemory:
    """Funders seen behind launch wallets, kept across coins and restarts.

    File layout: ``{funder: {"mints": [...], "wallets": [...]}}``, newest last,
    each list capped. Write failures are logged and ignored: losing the memory
    costs only recall, never a wrong buy.
    """

    MAX_FUNDERS = 5000
    MAX_PER_FUNDER = 50

    def __init__(self, path: Path | str | None, hub_wallets: int = 30) -> None:
        self.path = Path(path) if path else None
        self.hub_wallets = hub_wallets
        self.funders: dict[str, dict[str, list[str]]] = {}
        if self.path and self.path.exists():
            try:
                self.funders = json.loads(self.path.read_text())
            except (OSError, ValueError) as e:
                logger.warning(
                    f"[crew] could not read {self.path}: {e}; starting empty"
                )

    def is_service(self, funder: str) -> bool:
        """Exchange or hub: tops up too many wallets to mean anything."""
        if funder in EXCHANGES:
            return True
        return len(self.funders.get(funder, {}).get("wallets", ())) >= self.hub_wallets

    def earlier_launches(self, funder: str, mint: str) -> list[str]:
        return [m for m in self.funders.get(funder, {}).get("mints", ()) if m != mint]

    def record(self, mint: str, funded: dict[str, str]) -> None:
        """Remember ``funded`` ({wallet: funder}) for this coin and save."""
        for wallet, funder in funded.items():
            entry = self.funders.setdefault(funder, {"mints": [], "wallets": []})
            for key, value in (("mints", mint), ("wallets", wallet)):
                if value not in entry[key]:
                    entry[key].append(value)
                    del entry[key][: -self.MAX_PER_FUNDER]
            self.funders[funder] = self.funders.pop(funder)  # most recent last
        while len(self.funders) > self.MAX_FUNDERS:
            self.funders.pop(next(iter(self.funders)))
        if not self.path:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.funders))
            tmp.replace(self.path)
        except OSError as e:
            logger.warning(f"[crew] could not save {self.path}: {e}")


def judge_funding(
    mint: str,
    creator: str | None,
    funded: dict[str, str],
    memory: CrewMemory,
    *,
    max_shared_funder_wallets: int,
    max_crew_earlier_launches: int,
) -> str | None:
    """Reason to skip from who funded the launch wallets, or None.

    ``funded`` maps each traced wallet (dev and launch-block buyers) to its
    funder. Skips when one funder (not a service) funded more than
    ``max_shared_funder_wallets`` of them, when the dev funded a launch-block
    buyer, or when a funder was behind more than ``max_crew_earlier_launches``
    earlier coins. Call before ``memory.record`` for the same coin.
    """
    if creator and any(f == creator for w, f in funded.items() if w != creator):
        return "dev funded a launch-block buyer (linked wallets)"
    counts = Counter(f for f in funded.values() if not memory.is_service(f))
    if counts:
        funder, n = counts.most_common(1)[0]
        if max_shared_funder_wallets > 0 and n > max_shared_funder_wallets:
            return f"{n} launch wallets funded by one wallet {funder[:6]}... (linked wallets)"
    if max_crew_earlier_launches >= 0:
        for funder in counts:
            earlier = memory.earlier_launches(funder, mint)
            if len(earlier) > max_crew_earlier_launches:
                return (
                    f"funder {funder[:6]}... was behind {len(earlier)} earlier launches "
                    "(repeat crew)"
                )
    return None


async def trace_funders(
    post_rpc: PostRpc,
    wallets: dict[str, str],
    history: int = 5,
    concurrency: int = 4,
) -> dict[str, str]:
    """Funder of each wallet, read from its transactions before a given signature.

    ``wallets`` maps wallet -> the signature to look back from (its launch buy,
    or the create for the dev). ``post_rpc`` is ``SolanaClient.post_rpc``. A
    wallet whose funder can't be found is left out; nothing is guessed.
    """
    sem = asyncio.Semaphore(concurrency)

    async def one(wallet: str, before: str) -> tuple[str, str | None]:
        async with sem:
            resp = await post_rpc(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignaturesForAddress",
                    "params": [
                        wallet,
                        {"before": before, "limit": history, "commitment": "confirmed"},
                    ],
                },
                max_retries=1,
                deadline_seconds=3,
            )
            for s in (resp or {}).get("result") or []:
                if s.get("err") is not None:
                    continue
                tx = await get_parsed_tx(post_rpc, s["signature"])
                found = funding_transfer(tx, wallet)
                if found and found[0]:
                    return wallet, found[0]
            return wallet, None

    results = await asyncio.gather(
        *(one(w, b) for w, b in wallets.items()), return_exceptions=True
    )
    return {
        w: f for r in results if not isinstance(r, BaseException) for w, f in [r] if f
    }


async def get_parsed_tx(post_rpc: PostRpc, signature: str) -> dict[str, Any] | None:
    resp = await post_rpc(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 1,
                },
            ],
        },
        max_retries=1,
        deadline_seconds=3,
    )
    return (resp or {}).get("result")


async def launch_block_buyers(
    post_rpc: PostRpc, block: LaunchBlock, limit: int, concurrency: int = 4
) -> dict[str, str]:
    """{buyer wallet: its launch-block signature} for up to ``limit`` transactions."""
    sem = asyncio.Semaphore(concurrency)

    async def one(sig: str) -> tuple[str | None, str]:
        async with sem:
            return fee_payer(await get_parsed_tx(post_rpc, sig)), sig

    results = await asyncio.gather(
        *(one(s) for s in block.others[:limit]), return_exceptions=True
    )
    out: dict[str, str] = {}
    for r in results:
        if not isinstance(r, BaseException) and r[0]:
            out.setdefault(r[0], r[1])
    return out
