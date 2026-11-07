"""Paper test: do volume-bot buy patterns time pump.fun runs and dumps?

Rock's observation: a run of identical bot buys ("0.01, 0.01" pennies, or a
repeated size like "0.07, 0.07") shows up right before a blitz of green or a
drop. This records every trade on a sample of new pump.fun coins and paper-buys
on each pattern so the outcomes can be compared with random entries.

1. Launches: PumpPortal's free create stream; one new coin is admitted every
   ADMIT_GAP seconds while fewer than MAX_COINS are tracked.
2. Tape: per coin, ``logsSubscribe`` on its bonding curve over the public WSS;
   each notification's pump.fun TradeEvent gives size, side, wallet and the
   curve reserves after the trade. No getTransaction calls.
3. Patterns, checked on every trade (kinds in ``detect``):
   penny_pair / penny_triple: 2 / 3 consecutive buys of the same size <= PENNY.
   same_pair / same_triple: the same for sizes above PENNY up to SAME_MAX.
   same_wallet_pair: one wallet buying the same size twice in a row.
   sell_pair: 2 consecutive sells of the same size (the "double sell"); paper
     long like the rest, so a negative result means it calls the drop.
   control: a random trade, the baseline every pattern is judged against.
4. Paper trade: PAPER_SIZE_SOL bought ENTRY_DELAY s after the signal at the
   reserves then (fees, latency, curve impact included), marked to its curve
   sell value on every later trade, several exit rules scored on one path.
5. Tapes of coins with >= TAPE_MIN_TRADES trades go to tapes.jsonl for offline
   pattern mining.

Moves no funds and never loads a wallet. Run ``python -m volbot.engine`` from
``src/``. Env: VOLBOT_DATA_DIR, PAPER_SIZE_SOL.
"""

from __future__ import annotations

import asyncio
import base64
import csv
import json
import logging
import logging.handlers
import os
import random
import statistics
import struct
import sys
import time
from collections import Counter, defaultdict, deque
from datetime import UTC, datetime
from pathlib import Path

from devwatch.engine import (
    CURVE_FEE,
    LATENCY,
    PUBLIC_WSS,
    PUMPPORTAL,
    b58encode,
    curve_buy_tokens,
    curve_sell_lamports,
)

logger = logging.getLogger("volbot")

DATA = Path(os.environ.get("VOLBOT_DATA_DIR", "trades/volbot"))
SIZE_SOL = float(os.environ.get("PAPER_SIZE_SOL", "0.2"))
MAX_COINS = int(os.environ.get("VOLBOT_MAX_COINS", "60"))  # subscriptions on one socket
ADMIT_GAP = float(os.environ.get("VOLBOT_ADMIT_GAP", "6"))  # seconds between new coins
SUB_GAP = 0.35  # public WSS closes the socket on subscription bursts
TRACK_SECONDS = 25 * 60
EARLY_CHECK_S, EARLY_MIN_TRADES = 120, 8  # drop quiet coins to free the slot
IDLE_DROP_S = 300
ENTRY_DELAY = 1.5  # see the signal, send, land: fill at the reserves this much later
PENNY = 0.02  # SOL
SAME_MAX = 0.5
SAME_TOL = 0.02  # relative size match
SAME_ABS = 0.0005  # SOL; absolute floor for the match on pennies
COOLDOWN_S = 45  # per coin per kind
CONTROL_P = 0.04  # chance a trade is a control entry
MAX_OPEN = 400
TAPE_MIN_TRADES = 15
TAPE_MAX_BYTES = 400_000_000

TRADE_DISC = bytes([189, 219, 127, 211, 78, 230, 97, 238])
TRADE_FMT = "<32sQQ?32sqQQQQ"  # mint sol tok is_buy user ts vsr vtr rsr rtr

# (name, take profit, stop loss, trail activate, trail distance, max hold s)
VARIANTS = [
    ("scalp", 0.10, 0.08, None, None, 60),
    ("quick", 0.20, 0.12, None, None, 180),
    ("tp40", 0.40, 0.20, None, None, 600),
    ("trail", None, 0.20, 0.15, 0.10, 900),
    ("hold30s", None, None, None, None, 30),
    ("hold2m", None, None, None, None, 120),
]
RULES = [v[0] for v in VARIANTS]
KINDS = [
    "penny_pair", "penny_triple", "same_pair", "same_triple",
    "same_wallet_pair", "sell_pair", "control",
]  # fmt: skip
RESULT_FIELDS = [
    "time_utc", "kind", "symbol", "mint", "coin_age_s", "size_sol", "trades_before",
    "entry_vq_sol", "peak_mult", "m10", "m30", "m60", "m180", "buys_next60",
    "sells_next60", "new_wallets_next60",
] + [f"{n}_{x}" for n in RULES for x in ("net", "why")]  # fmt: skip


def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def decode_trades(logs: list[str]) -> list[dict]:
    """pump.fun TradeEvents in a transaction's logs (SOL amounts in SOL)."""
    out = []
    for line in logs or ():
        if not line.startswith("Program data: "):
            continue
        try:
            raw = base64.b64decode(line[14:])
        except ValueError:
            continue
        if raw[:8] != TRADE_DISC or len(raw) < 8 + struct.calcsize(TRADE_FMT):
            continue
        mint, sol, tok, buy, user, _ts, vsr, vtr, _rsr, rtr = struct.unpack_from(
            TRADE_FMT, raw, 8
        )
        out.append(
            {
                "mint": b58encode(mint),
                "sol": sol / 1e9,
                "tok": tok,
                "buy": buy,
                "user": b58encode(user),
                "vq": vsr,
                "vt": vtr,
                "rt": rtr,
            }
        )
    return out


def same_size(a: float, b: float) -> bool:
    return abs(a - b) <= max(
        SAME_ABS if max(a, b) <= PENNY else 0.0, SAME_TOL * max(a, b)
    )


class Pos:
    """One paper buy: stake value multiple over time and per-rule exits."""

    def __init__(self, kind: str, coin: Coin, sig_trade: dict) -> None:
        self.kind, self.coin = kind, coin
        self.t_sig = time.time()
        self.age = self.t_sig - coin.t0
        self.size = sig_trade["sol"]
        self.trades_before = len(coin.tape)
        self.tokens = 0.0
        self.t0 = 0.0
        self.entry_vq = 0.0
        self.mults: list[tuple[float, float]] = []
        self.peak = 0.0
        self.exits: dict[str, tuple[str, float, float]] = {}
        self.idx0 = len(coin.tape)

    def fill(self) -> bool:
        c = self.coin
        if not c.vq or not c.vt:
            return False
        stake = SIZE_SOL * 1e9
        self.tokens = curve_buy_tokens(c.vt, c.vq, stake)
        if self.tokens <= 0:
            return False
        self.t0 = time.time()
        self.entry_vq = c.vq / 1e9
        self.mark()
        return True

    def mark(self) -> None:
        c = self.coin
        mult = curve_sell_lamports(c.vt, c.vq, self.tokens) / (SIZE_SOL * 1e9)
        el = time.time() - self.t0
        self.mults.append((el, mult))
        self.peak = max(self.peak, mult)
        for name, tp, sl, act, dist, hold in VARIANTS:
            if name in self.exits:
                continue
            r, why = mult - 1, None
            if tp is not None and r >= tp:
                why, r = "tp", tp
            elif sl is not None and r <= -sl:
                why = "sl"
            elif (
                act is not None
                and self.peak - 1 >= act
                and mult <= self.peak * (1 - dist)
            ):
                why = "trail"
            elif el >= hold:
                why = "time"
            if why:
                self.exits[name] = (why, r, el)

    @property
    def done(self) -> bool:
        return len(self.exits) == len(VARIANTS)

    def finish(self, why: str = "end") -> None:
        last = self.mults[-1][1] if self.mults else 1.0
        el = time.time() - self.t0
        for name in RULES:
            self.exits.setdefault(name, (why, last - 1, el))

    def at(self, sec: float) -> float | None:
        best = None
        for el, m in self.mults:
            if el <= sec:
                best = m
        return best


class Coin:
    def __init__(self, mint: str, curve: str, symbol: str) -> None:
        self.mint, self.curve, self.symbol = mint, curve, symbol
        self.t0 = time.time()
        self.tape: list[
            tuple[float, float, bool, str, int]
        ] = []  # t, sol, buy, user, vq
        self.vq = 0
        self.vt = 0
        self.last_trade = self.t0
        self.cool: dict[str, float] = {}
        self.wallets: set[str] = set()
        self.sub_id: int | None = None
        self.graduated = False


class VolBot:
    def __init__(self) -> None:
        self.coins: dict[str, Coin] = {}  # mint -> coin
        self.by_sub: dict[int, Coin] = {}
        self.pending_sub: dict[int, Coin] = {}
        self.pending_unsub: deque[int] = deque()
        self.sub_queue: deque[Coin] = deque()
        self.open: list[Pos] = []
        self.done: list[dict] = []
        self.stats: Counter = Counter()
        self.last_admit = 0.0
        self.req_id = 1000
        self.seen: deque[str] = deque(maxlen=20_000)
        self.seen_set: set[str] = set()

    # ------------------------------------------------------------ persistence
    def load(self) -> None:
        f = DATA / "results.csv"
        if f.exists():
            with f.open() as fh:
                self.done = list(csv.DictReader(fh))

    def record(self, p: Pos) -> None:
        tape = p.coin.tape[p.idx0 :]
        t_end = p.t_sig + 60
        nxt = [x for x in tape if x[0] <= t_end]
        before = {x[3] for x in p.coin.tape[: p.idx0]}
        row = {
            "time_utc": now_iso(),
            "kind": p.kind,
            "symbol": p.coin.symbol,
            "mint": p.coin.mint,
            "coin_age_s": round(p.age),
            "size_sol": round(p.size, 4),
            "trades_before": p.trades_before,
            "entry_vq_sol": round(p.entry_vq, 2),
            "peak_mult": round(p.peak, 3),
            "m10": _r(p.at(10)),
            "m30": _r(p.at(30)),
            "m60": _r(p.at(60)),
            "m180": _r(p.at(180)),
            "buys_next60": sum(1 for x in nxt if x[2]),
            "sells_next60": sum(1 for x in nxt if not x[2]),
            "new_wallets_next60": len({x[3] for x in nxt if x[2]} - before),
        }
        for name in RULES:
            why, net, _ = p.exits[name]
            row[f"{name}_net"] = round(net, 4)
            row[f"{name}_why"] = why
        self.done.append(row)
        f = DATA / "results.csv"
        new = not f.exists()
        with f.open("a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=RESULT_FIELDS)
            if new:
                w.writeheader()
            w.writerow(row)

    def save_tape(self, c: Coin) -> None:
        if len(c.tape) < TAPE_MIN_TRADES:
            return
        f = DATA / "tapes.jsonl"
        if f.exists() and f.stat().st_size > TAPE_MAX_BYTES:
            self.stats["tapes_skipped_full"] += 1
            return
        rec = {
            "mint": c.mint,
            "symbol": c.symbol,
            "start": datetime.fromtimestamp(c.t0, UTC).isoformat(timespec="seconds"),
            "graduated": c.graduated,
            "trades": [
                [round(t - c.t0, 2), round(s, 5), int(b), u[:8], round(vq / 1e9, 3)]
                for t, s, b, u, vq in c.tape
            ],
        }
        with f.open("a") as fh:
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
        self.stats["tapes"] += 1

    # ------------------------------------------------------------ patterns
    def detect(self, c: Coin) -> list[str]:
        t = c.tape
        if len(t) < 2:
            return []
        a, b = t[-2], t[-1]
        kinds = []
        if a[2] and b[2] and same_size(a[1], b[1]):
            small = b[1] <= PENNY
            if b[1] <= SAME_MAX:
                kinds.append("penny_pair" if small else "same_pair")
                if len(t) >= 3 and t[-3][2] and same_size(t[-3][1], b[1]):
                    kinds.append("penny_triple" if small else "same_triple")
                if a[3] == b[3]:
                    kinds.append("same_wallet_pair")
        if not a[2] and not b[2] and same_size(a[1], b[1]) and b[1] >= 0.01:
            kinds.append("sell_pair")
        if random.random() < CONTROL_P:
            kinds.append("control")
        return kinds

    def on_trade(self, c: Coin, ev: dict) -> None:
        now = time.time()
        c.tape.append((now, ev["sol"], ev["buy"], ev["user"], ev["vq"]))
        c.vq, c.vt = ev["vq"], ev["vt"]
        c.last_trade = now
        c.wallets.add(ev["user"])
        if ev["rt"] == 0:
            c.graduated = True
        self.stats["trades"] += 1
        for p in self.open:
            if p.coin is c and p.t0:
                p.mark()
        if c.graduated:
            return
        for kind in self.detect(c):
            if now - c.cool.get(kind, 0) < COOLDOWN_S or len(self.open) >= MAX_OPEN:
                continue
            c.cool[kind] = now
            self.stats[f"signal_{kind}"] += 1
            p = Pos(kind, c, ev)
            self.open.append(p)
            asyncio.get_running_loop().call_later(ENTRY_DELAY, self._fill, p)

    def _fill(self, p: Pos) -> None:
        if p not in self.open:
            return  # coin dropped before the fill
        if not p.fill():
            self.open.remove(p)
            self.stats["unfillable"] += 1

    def close_finished(self) -> None:
        for p in list(self.open):
            if p.t0:
                p.mark()  # time-based exits when the coin goes quiet
            if p.done:
                self.open.remove(p)
                self.record(p)

    # ------------------------------------------------------------ coins
    def admit(self, msg: dict) -> None:
        now = time.time()
        if (
            len(self.coins) >= MAX_COINS
            or now - self.last_admit < ADMIT_GAP
            or msg.get("pool", "pump") != "pump"
        ):
            return
        mint, curve = msg.get("mint"), msg.get("bondingCurveKey")
        if not mint or not curve or mint in self.coins:
            return
        self.last_admit = now
        c = Coin(mint, curve, str(msg.get("symbol", ""))[:16])
        self.coins[mint] = c
        self.sub_queue.append(c)
        self.stats["admitted"] += 1

    def drop(self, c: Coin, why: str) -> None:
        self.coins.pop(c.mint, None)
        if c.sub_id is not None:
            self.by_sub.pop(c.sub_id, None)
            self.pending_unsub.append(c.sub_id)
        for p in list(self.open):
            if p.coin is c:
                if p.t0:
                    p.finish("graduated" if c.graduated else "end")
                    self.record(p)
                self.open.remove(p)
        self.save_tape(c)
        self.stats[f"drop_{why}"] += 1

    def sweep(self) -> None:
        now = time.time()
        for c in list(self.coins.values()):
            age = now - c.t0
            busy = any(p.coin is c for p in self.open)
            if c.graduated:
                self.drop(c, "graduated")
            elif age > TRACK_SECONDS and not busy:
                self.drop(c, "age")
            elif age > EARLY_CHECK_S and len(c.tape) < EARLY_MIN_TRADES and not busy:
                self.drop(c, "quiet")
            elif now - c.last_trade > IDLE_DROP_S and not busy:
                self.drop(c, "idle")

    # ------------------------------------------------------------ sockets
    async def pumpportal_loop(self, websockets) -> None:  # noqa: ANN001
        while True:
            try:
                async with websockets.connect(PUMPPORTAL, ping_interval=20) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    logger.info("[pumpportal] connected")
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except ValueError:
                            continue
                        if isinstance(msg, dict) and msg.get("txType") == "create":
                            self.stats["launches"] += 1
                            self.admit(msg)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[pumpportal] {type(e).__name__} {e}; reconnecting")
            await asyncio.sleep(3)

    async def chain_loop(self, websockets) -> None:  # noqa: ANN001
        backoff = 10
        while True:
            sender = None
            try:
                async with websockets.connect(
                    PUBLIC_WSS, ping_interval=20, max_size=2**22
                ) as ws:
                    logger.info("[chain] connected")
                    # a fresh socket has no subscriptions: queue every tracked coin
                    self.by_sub.clear()
                    self.pending_sub.clear()
                    self.pending_unsub.clear()
                    self.sub_queue = deque(self.coins.values())
                    for c in self.coins.values():
                        c.sub_id = None
                    sender = asyncio.create_task(self._sender(ws))
                    async for raw in ws:
                        self._on_chain_msg(json.loads(raw))
                        backoff = 10
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    f"[chain] {type(e).__name__} {str(e)[:120]}; retry in {backoff}s"
                )
            finally:
                if sender:
                    sender.cancel()
            await asyncio.sleep(backoff + random.uniform(0, 5))
            backoff = min(backoff * 2, 600)

    async def _sender(self, ws) -> None:  # noqa: ANN001
        while True:
            if self.pending_unsub:
                sid = self.pending_unsub.popleft()
                self.req_id += 1
                await ws.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": self.req_id,
                            "method": "logsUnsubscribe",
                            "params": [sid],
                        }
                    )
                )
            elif self.sub_queue:
                c = self.sub_queue.popleft()
                if c.mint not in self.coins:
                    continue
                self.req_id += 1
                self.pending_sub[self.req_id] = c
                await ws.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": self.req_id,
                            "method": "logsSubscribe",
                            "params": [
                                {"mentions": [c.curve]},
                                {"commitment": "confirmed"},
                            ],
                        }
                    )
                )
            else:
                await asyncio.sleep(0.2)
                continue
            await asyncio.sleep(SUB_GAP)

    def _on_chain_msg(self, msg: dict) -> None:
        if "id" in msg:
            c = self.pending_sub.pop(msg.get("id"), None)
            if c is not None:
                if isinstance(msg.get("result"), int):
                    if c.mint in self.coins:
                        c.sub_id = msg["result"]
                        self.by_sub[c.sub_id] = c
                        self.stats["subs"] += 1
                    else:
                        self.pending_unsub.append(msg["result"])
                else:
                    self.stats["sub_errors"] += 1
                    if self.stats["sub_errors"] <= 5:
                        logger.warning(f"[chain] subscribe refused: {str(msg)[:200]}")
            return
        params = msg.get("params") or {}
        c = self.by_sub.get(params.get("subscription"))
        val = (params.get("result") or {}).get("value") or {}
        sig = val.get("signature")
        if not c or not sig or val.get("err") is not None or sig in self.seen_set:
            return
        if len(self.seen) == self.seen.maxlen:
            self.seen_set.discard(self.seen[0])
        self.seen.append(sig)
        self.seen_set.add(sig)
        for ev in decode_trades(val.get("logs") or []):
            if ev["mint"] == c.mint and ev["sol"] > 0:
                self.on_trade(c, ev)

    # ------------------------------------------------------------ report
    def report(self) -> str:
        rows = self.done
        lines = [
            f"Volume-bot pattern test (paper)  {now_iso()}",
            f"tracking {len(self.coins)} coins, {len(self.open)} open paper buys, "
            f"{len(rows)} results; stake {SIZE_SOL} SOL, fees+latency "
            f"{(CURVE_FEE + LATENCY) * 200:.1f}% round trip",
            f"counts: {dict(self.stats)}",
            "",
            "Net per trade after costs: mean / median / win%",
            f" {'kind':17}{'n':>5}{'coins':>6}  "
            + "".join(f"{r:>17}" for r in RULES)
            + f"{'peak>1.2':>9}{'m60 med':>8}",
        ]
        by = defaultdict(list)
        for r in rows:
            by[r["kind"]].append(r)
        for kind in KINDS:
            rs = by.get(kind)
            if not rs:
                continue
            cells = []
            for name in RULES:
                nets = [float(r[f"{name}_net"]) for r in rs]
                cells.append(
                    f"{statistics.mean(nets) * 100:+5.1f}/{statistics.median(nets) * 100:+5.1f}/"
                    f"{sum(n > 0 for n in nets) / len(nets):3.0%}"
                )
            peaks = sum(float(r["peak_mult"]) >= 1.2 for r in rs) / len(rs)
            m60 = [float(r["m60"]) for r in rs if r["m60"] not in ("", None)]
            lines.append(
                f" {kind:17}{len(rs):>5}{len({r['mint'] for r in rs}):>6}  "
                + "".join(f"{c:>17}" for c in cells)
                + f"{peaks:>9.0%}{(statistics.median(m60) - 1) * 100 if m60 else 0:>+7.1f}%"
            )
        lines += ["", "penny_pair by coin age (quick exit mean / n):"]
        for lo, hi in ((0, 60), (60, 180), (180, 600), (600, 99999)):
            for kind in ("penny_pair", "control"):
                rs = [r for r in by.get(kind, []) if lo <= float(r["coin_age_s"]) < hi]
                if rs:
                    m = statistics.mean(float(r["quick_net"]) for r in rs)
                    lines.append(
                        f"  age {lo:>4}-{hi:<5}s {kind:11} {m * 100:+6.1f}%  n {len(rs)}"
                    )
        lines.append(
            "(compare every pattern with control: only a gap over control is an edge)"
        )
        return "\n".join(lines)


def _r(x: float | None) -> str:
    return "" if x is None else f"{x:.3f}"


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

    vb = VolBot()
    vb.load()
    logger.info(f"volbot starting: {len(vb.done)} results so far")

    async def ticker() -> None:
        n = 0
        while True:
            await asyncio.sleep(2)
            try:
                vb.close_finished()
                vb.sweep()
                n += 1
                if n % 60 == 0:
                    (DATA / "report.txt").write_text(vb.report())
                    logger.info(
                        f"[status] coins {len(vb.coins)} subs {len(vb.by_sub)} open "
                        f"{len(vb.open)} results {len(vb.done)} trades {vb.stats['trades']}"
                    )
            except Exception:
                logger.exception("ticker failed")

    await asyncio.gather(
        ticker(), vb.pumpportal_loop(websockets), vb.chain_loop(websockets)
    )


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
