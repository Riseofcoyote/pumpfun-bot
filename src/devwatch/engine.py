"""Paper test: do the devs behind top pump.fun coins launch winners again?

1. Watchlist (every few hours): creators of the top pump.fun coins by volume on
   GeckoTerminal, read on chain from each coin's pool or curve account, and the
   wallet that first funded each creator.
2. Linked wallets: every watched creator or funder is followed with
   ``logsSubscribe``; a SOL transfer from one of them to a new wallet marks that
   wallet as linked for a week (devs launch again from fresh wallets).
3. Launches: PumpPortal's free create stream. A create by a watched creator,
   a watched funder, or a linked wallet is a signal; one random ordinary launch
   every few minutes is a control, tracked the same way.
4. Paper trade: 0.2 SOL bought on the curve ~2 s after the create (fees,
   latency and curve price impact included), marked to its sell value on the
   curve, or via Jupiter once graduated, for an hour. Several exit rules are
   scored on the same price path.

Moves no funds and never loads a wallet. Run ``python -m devwatch.engine`` from
``src/``. Env: DEVWATCH_DATA_DIR, SOLANA_NODE_RPC_ENDPOINT / _WSS_ENDPOINT
(optional; the public endpoint is tried first to spare the plan's quota).
"""

from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import json
import logging
import logging.handlers
import os
import random
import statistics
import struct
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

from trending import feeds

DATA = Path(os.environ.get("DEVWATCH_DATA_DIR", "trades/devwatch"))
SIZE_SOL = float(os.environ.get("PAPER_SIZE_SOL", "0.2"))
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"
PUBLIC_WSS = "wss://api.mainnet-beta.solana.com"
PUMPPORTAL = "wss://pumpportal.fun/api/data"
SOL_MINT = "So11111111111111111111111111111111111111112"
DEFAULT_KEY = "11111111111111111111111111111111"

CURVE_FEE = 0.0125  # pump.fun protocol + creator fee per side
LATENCY = 0.01  # price moving against us between signal and fill, per side
GRAD_SIDE = 0.01  # per-side cost once trading on PumpSwap
# A graduated read above GRAD_SPIKE x the last value counts only after this many
# consecutive reads.
GRAD_SPIKE = 3.0
GRAD_SPIKE_READS = 3
ENTRY_DELAY = 2.0  # seconds from create to our simulated buy
TRACK_SECONDS = int(
    os.environ.get("DEVWATCH_TRACK_SECONDS", "3600")
)  # shorter only in smoke tests
CONTROL_EVERY = 240  # seconds between random control launches
REFRESH_HOURS = 2
TOP_PAGES = 10  # 20 pools per page (GeckoTerminal maximum)
MIN_TOP_MCAP = 150_000
MAX_CREATORS = 150
MAX_FUNDERS = 100
LINK_DAYS = 7
MIN_LINK_SOL = 0.05
HUB_TRANSFERS_PER_DAY = 25
WALLETS_PER_SOCKET = 100
# The public WSS answers 429 past a handful of connections per IP, and the KOL
# watch shares this machine's IP with three sockets of its own.
MAX_SOCKETS = 2
SUB_GAP = 0.3  # seconds between logsSubscribe requests on one socket
RESUB_MIN_S = 1800  # least time between watchlist-driven resubscriptions
# Launches tracked per creator per UTC day; later ones are counted, not tracked,
# so one serial launcher cannot dominate a signal type's sample.
MAX_SIGNALS_PER_CREATOR_DAY = 3
# Creators of coins seen graduating live (PumpPortal migration stream), kept for
# same-wallet matching; the oldest are dropped past this many.
MAX_GRAD_CREATORS = 5000

# Exit rules scored on every price path: (name, take profit, stop loss, trail
# activate, trail distance, max hold seconds). Multiples are of the stake.
VARIANTS = [
    ("quick", 0.25, 0.15, None, None, 180),
    ("tp50", 0.50, 0.30, None, None, 600),
    ("tp100", 1.00, 0.40, None, None, 1800),
    ("tp200", 2.00, 0.50, None, None, 3600),
    ("trail", None, 0.35, 0.30, 0.20, 3600),
    ("hold60", None, None, None, None, 3600),
]

# The same rules entered late, only on launches still worth at least `floor` of
# the stake at `delay` seconds; most grad_dev launches are dumped within minutes.
# (name, delay s, floor, take profit, stop loss, trail activate, trail distance,
# max hold s from create). A launch below the floor is "skip": no trade.
DELAYED = [
    ("d5_trail", 300, 0.6, None, 0.35, 0.30, 0.20, 3600),
    ("d5_hold", 300, 0.6, None, None, None, None, 3600),
    ("d15_trail", 900, 0.6, None, 0.35, 0.30, 0.20, 3600),
    ("d5_tp100", 300, 0.6, 1.00, 0.35, None, None, 3600),
]
# A late entry pays its own buy and sell fee and latency on the value ratio.
LATE_COST = ((1 - CURVE_FEE) * (1 - LATENCY)) ** 2
BASE_RULES = {v[0] for v in VARIANTS}
RULE_NAMES = [v[0] for v in VARIANTS] + [v[0] for v in DELAYED]
RESULT_FIELDS = [
    "time_utc", "kind", "symbol", "mint", "creator", "via", "source_mcap",
    "graduated", "peak_mult", "mult_m5", "mult_m15", "mult_m30", "mult_m60",
] + [f"{n}_{x}" for n in RULE_NAMES for x in ("net", "why")] + ["gap_min", "end"]  # fmt: skip
# A signal whose creator launched again within this many minutes is a "burst"
# relaunch; the report splits grad_dev on it.
BURST_MIN = 10.0

EXCHANGES = {
    "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9",
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S",
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS",
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE",
    "2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm",
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2",
    "5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD",
    "is6MTRHEgyFLNTfYcuV4QBWLjrZBfmhVNYR6ccgr8KV",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5",
    "BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6",
    "G2YxRa6wt1qePMwfJzdXZG62ej4qaTC7YURzuh2Lwd3t",
    "ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ",
    "u6PJ8DtQuPFnfmwHbGFULQ4u4EgjDiyYKjVEsynXq2w",
}

logger = logging.getLogger("devwatch")

# ---------------------------------------------------------------- base58
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(b: bytes) -> str:
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    return "1" * (len(b) - len(b.lstrip(b"\0"))) + out


def b58decode(s: str) -> bytes:
    n = 0
    for ch in s:
        n = n * 58 + _B58.index(ch)
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\0" * (len(s) - len(s.lstrip("1"))) + body


_P = 2**255 - 19
_D = -121665 * pow(121666, _P - 2, _P) % _P


def _on_curve(b: bytes) -> bool:
    """Whether 32 bytes decode to a point on ed25519 (PDAs must not)."""
    y = int.from_bytes(b, "little") & ((1 << 255) - 1)
    if y >= _P:
        return False
    u = (y * y - 1) % _P
    v = (_D * y * y + 1) % _P
    x2 = u * pow(v, _P - 2, _P) % _P
    if x2 == 0:
        return True
    return pow(x2, (_P - 1) // 2, _P) == 1


def find_pda(seeds: list[bytes], program: str) -> str:
    prog = b58decode(program)
    for bump in range(255, -1, -1):
        h = hashlib.sha256(
            b"".join(seeds) + bytes([bump]) + prog + b"ProgramDerivedAddress"
        ).digest()
        if not _on_curve(h):
            return b58encode(h)
    raise ValueError("no PDA")


PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


def curve_address(mint: str) -> str:
    return find_pda([b"bonding-curve", b58decode(mint)], PUMP_PROGRAM)


def is_pump_dex(rec: dict) -> bool:
    return rec["mint"].endswith("pump") or "pump" in rec["dex"]


# ---------------------------------------------------------------- account decoding
CURVE_DISC = bytes([23, 183, 248, 55, 96, 216, 172, 96])
POOL_DISC = bytes([241, 154, 109, 4, 17, 177, 109, 188])


def decode_curve(data: bytes) -> dict | None:
    """pump.fun BondingCurve: reserves (raw units), complete, creator, quote mint."""
    if len(data) < 115 or data[:8] != CURVE_DISC:
        return None
    vt, vq, rt, rq, supply = struct.unpack_from("<5Q", data, 8)
    return {
        "vt": vt,
        "vq": vq,
        "complete": bool(data[48]),
        "creator": b58encode(data[49:81]),
        "quote_mint": b58encode(data[83:115]),
    }


def pool_coin_creator(data: bytes) -> str | None:
    """PumpSwap Pool.coin_creator, or None if not a canonical pump.fun pool."""
    if len(data) < 243 or data[:8] != POOL_DISC:
        return None
    c = b58encode(data[211:243])
    return None if c == DEFAULT_KEY else c


def creator_from_account(data: bytes) -> str | None:
    cur = decode_curve(data)
    return cur["creator"] if cur else pool_coin_creator(data)


# ---------------------------------------------------------------- curve math
def curve_buy_tokens(vt: int, vq: int, lamports_in: float) -> float:
    net = lamports_in * (1 - CURVE_FEE)
    return (vt - vt * vq / (vq + net)) * (1 - LATENCY)


def curve_sell_lamports(vt: int, vq: int, tokens: float) -> float:
    out = vq - vt * vq / (vt + tokens)
    return out * (1 - CURVE_FEE) * (1 - LATENCY)


class Path_:
    """A tracked launch: stake value multiple over time and per-variant exits."""

    def __init__(self, kind: str, meta: dict, *, late: bool = False) -> None:
        """``late`` adds the DELAYED rules; other testers subclass without them."""
        self.kind, self.meta = kind, meta
        self.late_rules = DELAYED if late else []
        self.t0 = time.time()
        self.mults: list[tuple[float, float]] = []
        self.peak = 0.0
        self.exits: dict[str, tuple[str, float, float]] = {}
        self.late: dict[str, list[float]] = {}  # name -> [entry mult, peak mult]

    def mark(self, mult: float) -> None:
        el = time.time() - self.t0
        self.mults.append((el, mult))
        self.peak = max(self.peak, mult)
        for name, tp, sl, act, dist, hold in VARIANTS:
            if name in self.exits:
                continue
            r = mult - 1
            why = None
            if tp is not None and r >= tp:
                why, val = "tp", 1 + tp  # a limit fill at target, never above
            elif sl is not None and r <= -sl:
                why, val = "sl", mult
            elif (
                act is not None
                and self.peak - 1 >= act
                and mult <= self.peak * (1 - dist)
            ):
                why, val = "trail", mult
            elif el >= hold:
                why, val = "time", mult
            if why:
                self.exits[name] = (why, val - 1, el)
        self._mark_late(el, mult)

    def _mark_late(self, el: float, mult: float) -> None:
        for name, delay, floor, tp, sl, act, dist, hold in self.late_rules:
            if name in self.exits or el < delay:
                continue
            if name not in self.late:
                if mult < floor or mult <= 0:
                    self.exits[name] = ("skip", 0.0, el)
                else:
                    self.late[name] = [mult, mult]
                continue
            base, peak = self.late[name]
            peak = self.late[name][1] = max(peak, mult)
            r = mult / base * LATE_COST - 1
            pr = peak / base * LATE_COST - 1
            why = None
            if tp is not None and r >= tp:
                why, r = "tp", tp
            elif sl is not None and r <= -sl:
                why = "sl"
            elif act is not None and pr >= act and mult <= peak * (1 - dist):
                why = "trail"
            elif el >= hold:
                why = "time"
            if why:
                self.exits[name] = (why, r, el)

    def finish(self) -> None:
        last = self.mults[-1][1] if self.mults else 0.0
        el = time.time() - self.t0
        for name, *_ in VARIANTS:
            self.exits.setdefault(name, ("end", last - 1, el))
        for name, *_ in self.late_rules:
            if name in self.late:
                net = last / self.late[name][0] * LATE_COST - 1
                self.exits.setdefault(name, ("end", net, el))
            else:
                self.exits.setdefault(name, ("skip", 0.0, el))

    def at(self, sec: float) -> float | None:
        best = None
        for el, m in self.mults:
            if el <= sec:
                best = m
        return best


# ---------------------------------------------------------------- RPC
class Rpc:
    """JSON-RPC over urllib in a thread; public endpoint first, plan endpoint on failure."""

    def __init__(self) -> None:
        plan = os.environ.get("SOLANA_NODE_RPC_ENDPOINT")
        self.urls = [PUBLIC_RPC] + ([plan] if plan else [])
        self.gate = asyncio.Semaphore(4)
        self.calls = Counter()

    def _post(self, url: str, body: bytes) -> dict | None:
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:  # noqa: S310
            return json.loads(r.read().decode())

    async def call(self, method: str, params: list) -> object:
        body = json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        ).encode()
        async with self.gate:
            for i, url in enumerate(self.urls):
                try:
                    doc = await asyncio.to_thread(self._post, url, body)
                    self.calls["public" if i == 0 else "plan"] += 1
                    if doc and "error" not in doc:
                        return doc.get("result")
                except (urllib.error.URLError, TimeoutError, ValueError, OSError):
                    pass
                await asyncio.sleep(0.3)
        return None

    async def account(self, address: str) -> bytes | None:
        res = await self.call(
            "getAccountInfo",
            [address, {"encoding": "base64", "commitment": "confirmed"}],
        )
        try:
            return base64.b64decode(res["value"]["data"][0])  # type: ignore[index]
        except (TypeError, KeyError, IndexError, ValueError):
            return None

    async def tx(self, sig: str) -> dict | None:
        res = await self.call(
            "getTransaction",
            [
                sig,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 1,
                },
            ],
        )
        return res if isinstance(res, dict) else None


def system_transfers(tx: dict | None) -> list[tuple[str, str, float]]:
    """(source, destination, SOL) of every successful system transfer in a parsed tx."""
    if not tx or (tx.get("meta") or {}).get("err") is not None:
        return []
    ixs = list(
        (tx.get("transaction") or {}).get("message", {}).get("instructions") or []
    )
    for g in (tx.get("meta") or {}).get("innerInstructions") or []:
        ixs += g.get("instructions") or []
    out = []
    for ix in ixs:
        p = ix.get("parsed")
        if ix.get("program") != "system" or not isinstance(p, dict):
            continue
        if p.get("type") not in ("transfer", "transferWithSeed"):
            continue
        info = p.get("info") or {}
        out.append(
            (
                info.get("source"),
                info.get("destination"),
                int(info.get("lamports") or 0) / 1e9,
            )
        )
    return out


def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------- engine
class DevWatch:
    def __init__(self, rpc: Rpc) -> None:
        self.rpc = rpc
        self.coins: dict[str, dict] = {}  # mint -> {symbol, pool, creator, mcap}
        self.creators: dict[str, dict] = {}  # wallet -> {coins, best_mcap, funder}
        self.funders: dict[str, dict] = {}  # wallet -> {creators}
        self.linked: dict[str, dict] = {}  # wallet -> {via, via_kind, at, sol}
        self.hubs: set[str] = set()
        self.transfers_today: Counter = Counter()
        self.creator_signals: Counter = Counter()  # (utc day, creator) -> launches
        # signal creator -> epoch of their last create
        self.last_launch: dict[str, float] = {}
        self.day = datetime.now(UTC).date().isoformat()
        self.seen_sigs: set[str] = set()
        self.tx_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        self.open: list[Path_] = []
        self.tasks: set[asyncio.Task] = set()
        self.done: list[dict] = []
        self.stats = Counter()
        self.last_control = 0.0
        self.watch_version = 0
        self.sol_usd = 150.0
        self.started = now_iso()

    # ----- persistence
    def save(self) -> None:
        state = {
            k: getattr(self, k)
            for k in ("coins", "creators", "funders", "linked", "started")
        }
        state["hubs"] = sorted(self.hubs)
        state["stats"] = dict(self.stats)
        tmp = DATA / "state.json.tmp"
        tmp.write_text(json.dumps(state))
        tmp.replace(DATA / "state.json")
        (DATA / "report.txt").write_text(self.report() + "\n")

    def load(self) -> None:
        p = DATA / "state.json"
        if p.exists():
            try:
                st = json.loads(p.read_text())
                for k in ("coins", "creators", "funders", "linked", "started"):
                    setattr(self, k, st.get(k, getattr(self, k)))
                self.hubs = set(st.get("hubs", []))
                self.stats = Counter(st.get("stats", {}))
            except (OSError, ValueError) as e:
                logger.warning(f"state unreadable ({e}); fresh start")
        rp = DATA / "results.csv"
        if rp.exists():
            with rp.open() as f:
                reader = csv.DictReader(f)
                self.done = list(reader)
                old = reader.fieldnames or []
            if old != RESULT_FIELDS:
                # New exit rules add columns; rewrite under the new header, old
                # rows keeping blanks there, so appended rows stay aligned.
                tmp = DATA / "results.csv.tmp"
                with tmp.open("w", newline="") as f:
                    w = csv.DictWriter(
                        f, fieldnames=RESULT_FIELDS, extrasaction="ignore"
                    )
                    w.writeheader()
                    for r in self.done:
                        w.writerow({k: r.get(k, "") for k in RESULT_FIELDS})
                tmp.replace(rp)
                logger.warning(
                    f"[results] header migrated ({len(self.done)} rows kept)"
                )

    # ----- watchlist
    def watched(self) -> dict[str, str]:
        """Wallets to follow on chain -> 'creator' or 'funder'.

        Top-coin creators come first by mcap; graduate creators (mcap 0) fill the
        remaining slots, newest first.
        """
        cs = sorted(
            (c for c in self.creators if c not in self.hubs),
            key=lambda c: (
                -self.creators[c]["best_mcap"],
                -self.creators[c].get("added", 0),
            ),
        )[:MAX_CREATORS]
        fs = sorted(
            (f for f in self.funders if f not in self.hubs),
            key=lambda f: -len(self.funders[f]["creators"]),
        )[:MAX_FUNDERS]
        out = dict.fromkeys(fs, "funder")
        out.update(dict.fromkeys(cs, "creator"))
        return out

    async def refresh_watchlist(self) -> None:
        pools: list[dict] = []
        for dex in ("pumpswap", "pump-fun"):
            for page in range(1, TOP_PAGES + 1):
                url = f"{feeds.GT}/networks/solana/dexes/{dex}/pools?sort=h24_volume_usd_desc&page={page}"
                doc = await asyncio.to_thread(
                    feeds.get_json, url, feeds.GT_GATE, feeds.GT_HDR
                )
                for p in (doc or {}).get("data") or []:
                    rec = feeds._parse_gt_pool(p)  # noqa: SLF001
                    # pump.fun origin is checked on chain (creator_of), not by suffix.
                    if (
                        rec
                        and rec["mcap"] >= MIN_TOP_MCAP
                        and rec["vol"]["h24"] >= 50_000
                    ):
                        pools.append(rec)
                if dex == "pump-fun" and page >= 2:
                    break
        # Coins the trending/top-volume lists carry right now, too.
        snap = await asyncio.to_thread(feeds.discover)
        pools += [
            r for r in snap.values() if r["mcap"] >= MIN_TOP_MCAP and is_pump_dex(r)
        ]
        new_creators = []
        for rec in pools:
            coin = self.coins.get(rec["mint"])
            if coin:
                coin["mcap"] = max(coin["mcap"], rec["mcap"])
                if coin["creator"] in self.creators:
                    c = self.creators[coin["creator"]]
                    c["best_mcap"] = max(c["best_mcap"], coin["mcap"])
                    if c.get("src") == "grad":
                        c["src"] = "top"
                        if c["funder"] == "":
                            c["funder"] = None
                            new_creators.append(coin["creator"])
                continue
            creator = await self.creator_of(rec["mint"], rec["pool"])
            if not creator or creator in EXCHANGES:
                self.stats["no_creator"] += 1
                continue
            self.coins[rec["mint"]] = {
                "symbol": rec["symbol"],
                "pool": rec["pool"],
                "creator": creator,
                "mcap": rec["mcap"],
                "seen": now_iso(),
            }
            c = self.creators.setdefault(
                creator, {"coins": [], "best_mcap": 0, "funder": None}
            )
            c["coins"].append(rec["mint"])
            c["best_mcap"] = max(c["best_mcap"], rec["mcap"])
            if c.get("src") == "grad" and c["funder"] == "":
                c["funder"] = None  # graduates are not traced; top creators are
            c["src"] = "top"
            if len(c["coins"]) > 8:
                self.hubs.add(creator)  # a program/fee address, not a person
            if c["funder"] is None:
                new_creators.append(creator)
        for creator in new_creators:
            funder = await self.trace_funder(creator)
            self.creators[creator]["funder"] = funder or ""
            if funder and funder not in EXCHANGES:
                self.funders.setdefault(funder, {"creators": []})["creators"].append(
                    creator
                )
        self.watch_version += 1
        w = self.watched()
        logger.warning(
            f"[watchlist] {len(pools)} top pump coins, {len(self.coins)} known, "
            f"{len(self.creators)} creators, {len(self.funders)} funders, "
            f"watching {len(w)} wallets, {len(self.linked)} linked, {len(self.hubs)} hubs"
        )

    async def creator_of(self, mint: str, pool: str) -> str | None:
        """The pump.fun creator: from the coin's bonding curve, else its pool.

        None for a coin that never had a pump.fun curve.
        """
        data = await self.rpc.account(curve_address(mint))
        cur = decode_curve(data) if data else None
        if cur and cur["creator"] != DEFAULT_KEY:
            return cur["creator"]
        if not pool:
            return None
        data = await self.rpc.account(pool)
        return pool_coin_creator(data) if data else None

    async def add_graduate(self, mint: str, pool: str) -> None:
        """Remember the creator of a coin that just graduated off its curve."""
        if not mint or mint in self.coins:
            return
        creator = await self.creator_of(mint, pool)
        if not creator or creator in EXCHANGES or creator in self.hubs:
            self.stats["grad_no_creator"] += 1
            return
        self.coins[mint] = {
            "symbol": "",
            "pool": pool,
            "creator": creator,
            "mcap": 0,
            "seen": now_iso(),
            "src": "grad",
        }
        c = self.creators.setdefault(
            creator,
            {"coins": [], "best_mcap": 0, "funder": "", "src": "grad"},
        )
        c["coins"].append(mint)
        c.setdefault("added", time.time())
        if len(c["coins"]) > 8:
            self.hubs.add(creator)
        self.stats["graduates"] += 1
        logger.info(f"[grad] {mint} creator {creator[:8]} ({len(c['coins'])} coins)")
        grads = [k for k, v in self.creators.items() if v.get("src") == "grad"]
        if len(grads) > MAX_GRAD_CREATORS:
            grads.sort(key=lambda k: self.creators[k].get("added", 0))
            for k in grads[: len(grads) - MAX_GRAD_CREATORS]:
                for m in self.creators.pop(k)["coins"]:
                    self.coins.pop(m, None)

    async def trace_funder(self, wallet: str) -> str | None:
        """The wallet that sent ``wallet`` its first real SOL, from its oldest txs."""
        sigs = await self.rpc.call("getSignaturesForAddress", [wallet, {"limit": 1000}])
        if not isinstance(sigs, list) or not sigs or len(sigs) >= 1000:
            return None  # too busy to reach its first funding in one page
        for s in reversed(sigs[-6:]):
            if s.get("err") is not None:
                continue
            for src, dst, sol in system_transfers(await self.rpc.tx(s["signature"])):
                if dst == wallet and sol >= 0.01 and src != wallet:
                    return src
        return None

    # ----- linked wallets
    async def logs_loop(self, websockets) -> None:  # noqa: ANN001
        """Follow the watched wallets, WALLETS_PER_SOCKET per public-WSS connection.

        At most MAX_SOCKETS connections; wallets past that budget are not followed.

        The public endpoint closes a socket with 1013 when subscriptions arrive in a
        burst, so each socket paces them SUB_GAP apart and backs off on errors. The
        plan WSS refuses logsSubscribe (403), so it is not used here. A watchlist
        change restarts the sockets at most every RESUB_MIN_S.
        """
        while True:
            ws_ = self.watched()
            # creators before funders when the socket budget truncates the list
            w = sorted(ws_, key=lambda a: ws_[a] != "creator")
            w = w[: WALLETS_PER_SOCKET * MAX_SOCKETS]
            version = self.watch_version
            if not w:
                await asyncio.sleep(30)
                continue
            chunks = [
                w[i : i + WALLETS_PER_SOCKET]
                for i in range(0, len(w), WALLETS_PER_SOCKET)
            ]
            socks = []
            for idx, chunk in enumerate(chunks):
                socks.append(
                    asyncio.create_task(self.logs_socket(websockets, idx, chunk))
                )
                await asyncio.sleep(len(chunk) * SUB_GAP + 5)
            logger.info(f"[logs] following {len(w)} wallets on {len(chunks)} sockets")
            started = time.time()
            while self.watch_version == version or time.time() - started < RESUB_MIN_S:
                await asyncio.sleep(30)
            for t in socks:
                t.cancel()
            await asyncio.gather(*socks, return_exceptions=True)
            if len(self.seen_sigs) > 50_000:
                self.seen_sigs.clear()

    async def logs_socket(self, websockets, idx: int, wallets: list[str]) -> None:  # noqa: ANN001
        backoff = 30
        while True:
            sub_task = None
            try:
                async with websockets.connect(
                    PUBLIC_WSS, ping_interval=20, max_size=2**22
                ) as ws:
                    subs: dict[int, str] = {}
                    reqs = dict(enumerate(wallets, start=1))

                    async def subscribe(ws=ws, reqs=reqs) -> None:  # noqa: ANN001
                        for i, wallet in reqs.items():
                            await ws.send(
                                json.dumps(
                                    {
                                        "jsonrpc": "2.0",
                                        "id": i,
                                        "method": "logsSubscribe",
                                        "params": [
                                            {"mentions": [wallet]},
                                            {"commitment": "confirmed"},
                                        ],
                                    }
                                )
                            )
                            await asyncio.sleep(SUB_GAP)

                    sub_task = asyncio.create_task(subscribe())
                    async for raw in ws:
                        msg = json.loads(raw)
                        if "id" in msg and msg.get("id") in reqs:
                            if "result" in msg:
                                subs[msg["result"]] = reqs[msg["id"]]
                                self.stats["logs_subs"] += 1
                                backoff = 30
                            else:
                                self.stats["logs_sub_errors"] += 1
                            continue
                        params = msg.get("params") or {}
                        val = (params.get("result") or {}).get("value") or {}
                        wallet = subs.get(params.get("subscription"))
                        sig = val.get("signature")
                        if (
                            wallet
                            and sig
                            and val.get("err") is None
                            and sig not in self.seen_sigs
                        ):
                            self.seen_sigs.add(sig)
                            self.stats["logs_txs"] += 1
                            if self.tx_queue.full():
                                self.tx_queue.get_nowait()
                            self.tx_queue.put_nowait((wallet, sig))
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    f"[logs] socket {idx}: {type(e).__name__} {str(e)[:120]}; retry in {backoff}s"
                )
            finally:
                if sub_task:
                    sub_task.cancel()
            await asyncio.sleep(backoff + random.uniform(0, 20))
            backoff = min(backoff * 2, 900)

    async def tx_worker(self) -> None:
        while True:
            wallet, sig = await self.tx_queue.get()
            today = datetime.now(UTC).date().isoformat()
            if today != self.day:
                self.day, self.transfers_today = today, Counter()
            watched = self.watched()
            for src, dst, sol in system_transfers(await self.rpc.tx(sig)):
                if src != wallet or src not in watched or sol < MIN_LINK_SOL:
                    continue
                if dst in watched or dst in EXCHANGES or dst == src:
                    continue
                self.transfers_today[src] += 1
                if self.transfers_today[src] > HUB_TRANSFERS_PER_DAY:
                    if src not in self.hubs:
                        self.hubs.add(src)
                        self.watch_version += 1
                        logger.warning(
                            f"[logs] {src[:8]} funds too many wallets; treated as a service"
                        )
                    continue
                self.linked[dst] = {
                    "via": src,
                    "via_kind": watched[src],
                    "at": time.time(),
                    "sol": round(sol, 3),
                }
                self.stats["links"] += 1
                logger.info(
                    f"[link] {watched[src]} {src[:8]} sent {sol:.2f} SOL to {dst[:8]}"
                )
            cutoff = time.time() - LINK_DAYS * 86400
            if len(self.linked) > 5000 or random.random() < 0.01:
                self.linked = {k: v for k, v in self.linked.items() if v["at"] > cutoff}
            await asyncio.sleep(0.3)

    def spawn(self, coro) -> None:  # noqa: ANN001
        """Run a tracking task; the event loop keeps only weak references, so hold one."""
        task = asyncio.create_task(coro)
        self.tasks.add(task)

        def done(t: asyncio.Task) -> None:
            self.tasks.discard(t)
            if not t.cancelled() and t.exception():
                self.stats["track_errors"] += 1
                logger.error("[track] failed", exc_info=t.exception())

        task.add_done_callback(done)

    # ----- launches
    def classify(self, creator: str) -> tuple[str, dict] | None:
        if creator in self.hubs:
            return None
        if creator in self.creators:
            c = self.creators[creator]
            kind = "grad_dev" if c.get("src") == "grad" else "same_dev"
            return kind, {"source_mcap": c["best_mcap"], "via": creator}
        if creator in self.funders:
            return "funder_launch", {"via": creator}
        link = self.linked.get(creator)
        if link and link["at"] > time.time() - LINK_DAYS * 86400:
            via = link["via"]
            vc = self.creators.get(via, {})
            mcap = vc.get("best_mcap", 0)
            if link["via_kind"] != "creator":
                kind = "funder_new_wallet"
            elif vc.get("src") == "grad":
                kind = "grad_dev_new_wallet"
            else:
                kind = "dev_new_wallet"
            return kind, {"via": via, "source_mcap": mcap, "link_sol": link["sol"]}
        return None

    async def pumpportal_loop(self, websockets) -> None:  # noqa: ANN001
        while True:
            try:
                async with websockets.connect(PUMPPORTAL, ping_interval=20) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    await ws.send(json.dumps({"method": "subscribeMigration"}))
                    logger.info("[pumpportal] connected")
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except ValueError:
                            continue
                        if not isinstance(msg, dict):
                            continue
                        if msg.get("txType") in ("migrate", "migration"):
                            self.spawn(
                                self.add_graduate(
                                    str(msg.get("mint") or ""),
                                    str(msg.get("pool") or ""),
                                )
                            )
                            continue
                        if msg.get("txType") != "create":
                            if self.stats["other_msgs"] < 5:
                                logger.info(f"[pumpportal] other: {str(raw)[:300]}")
                            self.stats["other_msgs"] += 1
                            continue
                        if msg.get("pool", "pump") != "pump":
                            continue
                        self.stats["launches"] += 1
                        creator = str(msg.get("traderPublicKey") or "")
                        hit = self.classify(creator)
                        meta = {
                            "mint": msg.get("mint"),
                            "symbol": str(msg.get("symbol", ""))[:16],
                            "creator": creator,
                            "curve": msg.get("bondingCurveKey"),
                            "dev_buy_sol": msg.get("solAmount"),
                        }
                        if hit:
                            kind, extra = hit
                            now = time.time()
                            prev = self.last_launch.get(creator)
                            # Minutes since this creator's previous create, capped ones
                            # included; blank when none was seen since start.
                            extra["gap_min"] = (
                                round((now - prev) / 60, 1) if prev else ""
                            )
                            if len(self.last_launch) > 50_000:
                                self.last_launch.clear()
                            self.last_launch[creator] = now
                            key = (datetime.now(UTC).date().isoformat(), creator)
                            self.creator_signals[key] += 1
                            if self.creator_signals[key] > MAX_SIGNALS_PER_CREATOR_DAY:
                                self.stats[f"capped_{kind}"] += 1
                                continue
                            if len(self.creator_signals) > 20_000:
                                self.creator_signals.clear()
                            self.stats[f"signal_{kind}"] += 1
                            logger.warning(
                                f"[signal] {kind} {meta['symbol']} {meta['mint']} by {creator[:8]} {extra}"
                            )
                            self.spawn(self.track(kind, meta | extra))
                        elif time.time() - self.last_control >= CONTROL_EVERY:
                            self.last_control = time.time()
                            self.spawn(self.track("control", meta))
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[pumpportal] {type(e).__name__} {e}; reconnecting")
            await asyncio.sleep(3)

    async def track(self, kind: str, meta: dict) -> None:
        """Paper-buy one launch and follow its value for an hour."""
        curve_addr, mint = meta.get("curve"), meta.get("mint")
        if not curve_addr or not mint:
            return
        await asyncio.sleep(ENTRY_DELAY)
        # A curve created seconds ago is often not visible yet at "confirmed";
        # the fill uses the first read that succeeds (later = worse price).
        cur = None
        for _ in range(4):
            data = await self.rpc.account(curve_addr)
            cur = decode_curve(data) if data else None
            if cur:
                break
            await asyncio.sleep(1.5)
        if (
            not cur
            or cur["complete"]
            or cur["quote_mint"] not in (SOL_MINT, DEFAULT_KEY)
        ):
            self.stats["unfillable"] += 1
            return
        stake = SIZE_SOL * 1e9
        tokens = curve_buy_tokens(cur["vt"], cur["vq"], stake)
        if tokens <= 0:
            return
        path = Path_(
            kind, meta | {"entry_vq_sol": round(cur["vq"] / 1e9, 2)}, late=True
        )
        self.open.append(path)
        fails = 0
        last_val = curve_sell_lamports(cur["vt"], cur["vq"], tokens)
        path.mark(last_val / stake)
        spikes = 0
        while time.time() - path.t0 < TRACK_SECONDS and len(path.exits) < len(
            RULE_NAMES
        ):
            el = time.time() - path.t0
            await asyncio.sleep(4 if el < 600 else 15)
            data = await self.rpc.account(curve_addr)
            cur = decode_curve(data) if data else None
            if cur and not cur["complete"] and cur["vt"] > 0:
                val = curve_sell_lamports(cur["vt"], cur["vq"], tokens)
            elif cur and cur["complete"]:
                # Value the bag at what a sell would fetch: Jupiter's mid price on a
                # freshly migrated coin printed 30-160x spikes that went to ~0 a
                # read later, and ignored the impact of selling into a new pool.
                out = await asyncio.to_thread(
                    feeds.jup_sell_lamports, mint, int(tokens)
                )
                if not out:
                    fails += 1
                    if fails > 40:
                        path.meta["end"] = "unpriced"
                        break
                    continue
                val = out * (1 - LATENCY)
                path.meta["graduated"] = True
                if val > GRAD_SPIKE * last_val and spikes < GRAD_SPIKE_READS - 1:
                    spikes += 1  # a real move persists; a one-read print does not
                    continue
                spikes = 0
            else:
                fails += 1
                if fails > 40:
                    path.meta["end"] = "lost"
                    break
                continue
            fails = 0
            last_val = val
            path.mark(val / stake)
        path.finish()
        self.open.remove(path)
        self.record(path)

    def record(self, p: Path_) -> None:
        row = {
            "time_utc": now_iso(),
            "kind": p.kind,
            "symbol": p.meta.get("symbol"),
            "mint": p.meta.get("mint"),
            "creator": p.meta.get("creator"),
            "via": p.meta.get("via", ""),
            "source_mcap": round(p.meta.get("source_mcap", 0) or 0),
            "graduated": bool(p.meta.get("graduated")),
            "peak_mult": round(p.peak, 3),
            "gap_min": p.meta.get("gap_min", ""),
            # "unpriced": the curve completed and no sell quote came for 40 reads,
            # so the exits froze at the last curve value; "lost": curve unreadable.
            "end": p.meta.get("end", "ok"),
        }
        for sec, name in ((300, "m5"), (900, "m15"), (1800, "m30"), (3600, "m60")):
            v = p.at(sec)
            row[f"mult_{name}"] = round(v, 3) if v is not None else ""
        for name in RULE_NAMES:
            why, net, _el = p.exits[name]
            row[f"{name}_net"] = "" if why == "skip" else round(net, 4)
            row[f"{name}_why"] = why
        path = DATA / "results.csv"
        new = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=RESULT_FIELDS)
            if new:
                w.writeheader()
            w.writerow(row)
        self.done.append({k: str(v) for k, v in row.items()})
        logger.info(
            f"[result] {p.kind} {row['symbol']} peak x{p.peak:.2f} "
            + " ".join(f"{n}={p.exits[n][1]:+.0%}" for n in RULE_NAMES)
        )

    # ----- reporting
    @staticmethod
    def suspect(r: dict) -> bool:
        """A row whose value froze on a print we could not have sold at.

        Either tracking ended without a price (``end`` unpriced/lost), or, for rows
        from before ``end`` existed, an ungraduated path stuck at one multiple above
        2x from minute 5 to 60: the curve completed, no quote came, and the last
        curve read stood in for the whole hour.
        """
        if r.get("end") in ("unpriced", "lost"):
            return True
        if r.get("end"):
            return False
        try:
            m5, m60 = float(r["mult_m5"]), float(r["mult_m60"])
        except (KeyError, TypeError, ValueError):
            return False
        return r.get("graduated") not in ("True", True) and m5 > 2 and m5 == m60

    @classmethod
    def robust_lines(cls, by: dict[str, list[dict]]) -> list[str]:
        """Per type and rule: median and mean without the 3 best rows, suspect rows out."""
        rules = [n for n in ("trail", "hold60", "d5_trail", "d15_trail", "d5_tp100")
                 if n in RULE_NAMES]  # fmt: skip
        out = [
            "\nRobust view (suspect frozen prints dropped; median | mean without top 3 | n):",
            "  type              drop  " + "  ".join(f"{n:>20}" for n in rules),
        ]
        for kind, rs in by.items():
            ok = [r for r in rs if not cls.suspect(r)]
            cells = []
            for name in rules:
                vals = sorted(float(v) for r in ok if (v := r.get(f"{name}_net")))
                if len(vals) < 5:
                    cells.append(f"{'-':>20}")
                    continue
                cut = statistics.mean(vals[:-3])
                cell = f"{statistics.median(vals):+.1%} | {cut:+.1%} | {len(vals)}"
                cells.append(f"{cell:>20}")
            out.append(f"  {kind:17} {len(rs) - len(ok):4}  " + "  ".join(cells))
        return out

    @staticmethod
    def burst_lines(rows: list[dict]) -> list[str]:
        """grad_dev split by burst relaunch (creator's previous launch <= BURST_MIN ago).

        Rows written before gap_min existed fall back to the gap between the
        creator's recorded results, which end up to an hour after the launch.
        """
        last: dict[str, datetime] = {}
        groups: dict[str, list[dict]] = {"burst": [], "other": []}
        for r in rows:
            try:
                t = datetime.fromisoformat(r["time_utc"].replace("Z", "+00:00"))
            except (KeyError, ValueError):
                continue
            gap = r.get("gap_min")
            if gap in (None, ""):
                prev = last.get(r.get("creator", ""))
                gap = (t - prev).total_seconds() / 60 if prev else None
            last[r.get("creator", "")] = t
            burst = gap is not None and float(gap) <= BURST_MIN
            groups["burst" if burst else "other"].append(r)
        out = [
            f"\ngrad_dev split (burst = same creator launched again within {BURST_MIN:.0f} min):"
        ]
        for name, rs in groups.items():
            trail = sorted(float(v) for r in rs if (v := r.get("trail_net")))
            hold = [float(v) for r in rs if (v := r.get("hold60_net"))]
            if len(trail) < 5:
                continue
            ex_top3 = statistics.mean(trail[:-3])
            out.append(
                f"  {name:6} n={len(trail):4} devs={len({r.get('creator') for r in rs}):3}"
                f"  trail mean {statistics.mean(trail):+.1%} median {statistics.median(trail):+.1%}"
                f" without top 3 {ex_top3:+.1%}  hold60 mean {statistics.mean(hold):+.1%}"
            )
        return out

    def report(self) -> str:
        w = self.watched()
        lines = [
            f"Dev/funder watch (paper)  {now_iso()}  started {self.started}",
            f"watchlist: {len(self.coins)} coins, {len(self.creators)} creators "
            f"({sum(v.get('src') == 'grad' for v in self.creators.values())} from graduations), "
            f"{len(self.funders)} funders; following {sum(v == 'creator' for v in w.values())} creators + "
            f"{sum(v == 'funder' for v in w.values())} funders; {len(self.linked)} linked wallets; "
            f"{len(self.hubs)} hubs excluded",
            f"counts: {dict(self.stats)}  open: {len(self.open)}  rpc calls: {dict(self.rpc.calls)}",
            "",
        ]
        by = defaultdict(list)
        for r in self.done:
            by[r["kind"]].append(r)
        lines.append(
            f"Results per launch type, net per trade after costs ({SIZE_SOL} SOL paper stake):"
        )
        hdr = "  type               n  devs  peak>2x  grad  " + "  ".join(
            f"{n:>7}" for n in RULE_NAMES
        )
        lines.append(hdr)
        for kind in (
            "same_dev",
            "dev_new_wallet",
            "grad_dev",
            "grad_dev_new_wallet",
            "funder_launch",
            "funder_new_wallet",
            "control",
        ):
            rs = by.get(kind, [])
            if not rs:
                continue
            cells = []
            for name in RULE_NAMES:
                vals = [float(v) for r in rs if (v := r.get(f"{name}_net"))]
                cell = f"{statistics.mean(vals):+.1%}" if vals else "-"
                if vals and name not in BASE_RULES:
                    cell += f"/{len(vals)}"
                cells.append(f"{cell:>7}")
            p2 = sum(float(r["peak_mult"]) >= 2 for r in rs) / len(rs)
            gr = sum(r["graduated"] in ("True", True) for r in rs) / len(rs)
            lines.append(
                f"  {kind:17} {len(rs):3}  {len({r['creator'] for r in rs}):4}   {p2:6.0%}  {gr:4.0%}  "
                + "  ".join(cells)
            )
        lines.append(
            "  (means; one big winner can carry a column, so read them with peak>2x and devs;"
            " d5/d15 columns are mean/trades entered late, older rows have none)"
        )
        lines.extend(self.robust_lines(by))
        lines.extend(self.burst_lines(by.get("grad_dev", [])))
        sig = [r for r in self.done if r["kind"] != "control"]
        if sig:
            lines.append("\nRecent signals:")
            for r in sig[-12:]:
                lines.append(
                    f"  {r['time_utc']} {r['kind']:17} {r['symbol']:10} peak x{float(r['peak_mult']):.2f} "
                    f"m15 {r['mult_m15'] or '-'} via {str(r['via'])[:8]} src mcap ${int(float(r['source_mcap'] or 0)):,}"
                )
        return "\n".join(lines)


async def amain() -> None:
    import websockets  # noqa: PLC0415  (only on the Fly image / Actions)

    DATA.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = logging.handlers.RotatingFileHandler(
        DATA / "engine.log", maxBytes=20_000_000, backupCount=3
    )
    for h in (logging.StreamHandler(sys.stdout), fh):
        h.setFormatter(fmt)
        root.addHandler(h)
    for noisy in ("websockets", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    dw = DevWatch(Rpc())
    dw.load()
    logger.info(
        f"devwatch starting: {len(dw.creators)} creators known, {len(dw.done)} results"
    )

    async def refresher() -> None:
        while True:
            try:
                px = await asyncio.to_thread(feeds.sol_usd)
                if px:
                    dw.sol_usd = px
                await dw.refresh_watchlist()
            except Exception:
                logger.exception("[watchlist] refresh failed")
            await asyncio.sleep(REFRESH_HOURS * 3600)

    async def saver() -> None:
        while True:
            await asyncio.sleep(120)
            try:
                dw.save()
                logger.info(
                    f"[status] launches {dw.stats['launches']} open {len(dw.open)} "
                    f"results {len(dw.done)} links {len(dw.linked)}"
                )
            except Exception:
                logger.exception("save failed")

    await asyncio.gather(
        refresher(),
        saver(),
        dw.logs_loop(websockets),
        dw.tx_worker(),
        dw.pumpportal_loop(websockets),
    )


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
