"""Backtest range / mean-reversion rules on established Solana coins (paper only).

Universe: GeckoTerminal trending + top Solana pools that are at least 3 days
old, hold $100k+ liquidity, $0.5M-$200M market cap and moved under 30% in 24 h.
Each pool's 5-minute candles (up to 1000, ~3.5 days) are fetched and three rule
families are tested with costs: Bollinger-band dips, Donchian range-bottom
buys and RSI dips, each inside a "range regime" filter.

Walk-forward: one global parameter set per family is chosen on the first 60%
of every coin's candles (>= 30 trades, >= 60% wins) and scored on the last 40%.
Entries fill at the next candle's open; a stop and a target in the same candle
count as the stop. Survivorship caveat: the universe is coins alive today.

Run from the repo root: python tools/range_research.py
"""

from __future__ import annotations

import itertools
import statistics as st
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trending import feeds  # noqa: E402

SLIP = 0.003  # per side
TX = 0.002  # per side: network + priority fee on a 0.1 SOL position
DEX_FEE = {"pumpswap": 0.003, "raydium": 0.0025, "raydium-clmm": 0.0025, "orca": 0.003,
           "meteora": 0.005, "meteora-damm-v2": 0.005}


def universe() -> list[dict]:
    pools = dict(feeds.discover())
    for page in (1, 2, 3, 4, 5):
        doc = feeds.get_json(f"{feeds.GT}/networks/solana/pools?page={page}", feeds.GT_GATE)
        for p in (doc or {}).get("data") or []:
            rec = feeds._parse_gt_pool(p)
            if rec:
                pools.setdefault(rec["pool"], rec)
    out = []
    seen_mints = set()
    for r in pools.values():
        if r["mint"] in seen_mints:
            continue
        if r["age_h"] >= 72 and r["liq"] >= 100_000 and 5e5 <= r["mcap"] <= 2e8 and abs(r["pc"]["h24"]) <= 0.30:
            seen_mints.add(r["mint"])
            out.append(r)
    return out


def candles(pool: str) -> list[tuple[float, float, float, float, float]]:
    doc = feeds.get_json(
        f"{feeds.GT}/networks/solana/pools/{pool}/ohlcv/minute?aggregate=5&limit=1000&currency=usd",
        feeds.GT_GATE,
    )
    rows = (((doc or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
    rows = sorted((float(t), float(o), float(h), float(lo), float(c)) for t, o, h, lo, c, _v in rows)
    return [r for r in rows if r[4] > 0]


class Ind:
    """Per-coin indicator arrays, computed once and shared by every parameter set."""

    def __init__(self, c: list) -> None:
        self.c = c
        self.close = [r[4] for r in c]
        n = len(c)
        self.hi24, self.lo24 = self._roll(288, n)
        self._cache: dict = {}
        self.rsi = [None] * n
        for i in range(15, n):
            g = l_ = 0.0
            for j in range(i - 13, i + 1):
                d = self.close[j] - self.close[j - 1]
                g += max(d, 0)
                l_ += max(-d, 0)
            self.rsi[i] = 100.0 if l_ == 0 else 100 - 100 / (1 + g / l_)

    def _roll(self, w: int, n: int) -> tuple[list, list]:
        hi, lo = [0.0] * n, [0.0] * n
        for i in range(n):
            a = max(0, i - w + 1)
            hi[i] = max(r[2] for r in self.c[a:i + 1])
            lo[i] = min(r[3] for r in self.c[a:i + 1])
        return hi, lo

    def donch(self, w: int) -> tuple[list, list]:
        if ("d", w) not in self._cache:
            self._cache[("d", w)] = self._roll(w, len(self.c))
        return self._cache[("d", w)]

    def boll(self, w: int) -> tuple[list, list]:
        if ("b", w) not in self._cache:
            m, sd = [0.0] * len(self.c), [0.0] * len(self.c)
            for i in range(w - 1, len(self.c)):
                xs = self.close[i - w + 1:i + 1]
                mu = sum(xs) / w
                m[i], sd[i] = mu, (sum((x - mu) ** 2 for x in xs) / w) ** 0.5
            self._cache[("b", w)] = (m, sd)
        return self._cache[("b", w)]

    def regime(self, i: int, max_range: float, max_drift: float) -> bool:
        """Ranging: the last 24 h spans at most max_range and drifted at most max_drift."""
        if i < 288:
            return False
        return (self.hi24[i] / self.lo24[i] - 1 <= max_range
                and abs(self.close[i] / self.close[i - 288] - 1) <= max_drift)


def simulate(ind: Ind, start: int, end: int, family: str, p: dict, fee: float) -> list[float]:
    """Net returns of trades entered on bars [start, end)."""
    c, closes = ind.c, ind.close
    side = fee + SLIP + TX
    out = []
    i = max(start, 300)
    while i < end - 1:
        sig, target = False, None
        if ind.regime(i, p["range"], p["drift"]):
            if family == "boll":
                m, sd = ind.boll(p["n"])
                sig = sd[i] > 0 and closes[i] < m[i] - p["k"] * sd[i]
                target = m[i]
            elif family == "donch":
                his, los = ind.donch(p["n"])
                hi, lo = his[i], los[i]
                sig = 0.06 <= hi / lo - 1 <= 0.5 and closes[i] <= lo + p["pos"] * (hi - lo)
                target = lo + p["exit"] * (hi - lo)
            elif family == "rsi":
                v = ind.rsi[i]
                sig = v is not None and v < p["lo"]
        if not sig:
            i += 1
            continue
        entry = c[i + 1][1]
        if entry <= 0:
            i += 1
            continue
        tp = entry * (1 + p["tp"])
        if target is not None and target > entry * (1 + 2 * side):
            tp = min(tp, target)
        sl = entry * (1 - p["sl"])
        exit_px, j = None, i + 1
        while j < min(len(c), i + 1 + p["hold"]):
            _t, _o, h, lo, cl = c[j]
            if lo <= sl:
                exit_px = sl
                break
            if h >= tp:
                exit_px = tp
                break
            if family == "rsi" and ind.rsi[j] is not None and ind.rsi[j] > p["hi"]:
                exit_px = cl
                break
            j += 1
        if exit_px is None:
            j = min(len(c) - 1, i + p["hold"])
            exit_px = c[j][4]
        out.append(exit_px / entry * (1 - side) ** 2 - 1)
        i = j + 1
    return out


GRIDS = {
    "boll": [dict(n=n, k=k, tp=tp, sl=sl, hold=h, range=r, drift=d)
             for n, k, tp, sl, h, r, d in itertools.product(
                 (24, 48), (2.0, 2.5), (0.03, 0.06), (0.05, 0.10), (24, 72), (0.3, 0.6), (0.15, 0.3))],
    "donch": [dict(n=n, pos=pos, exit=ex, tp=tp, sl=sl, hold=h, range=r, drift=d)
              for n, pos, ex, tp, sl, h, r, d in itertools.product(
                  (48, 144), (0.15, 0.25), (0.5, 0.8), (0.10,), (0.05, 0.10), (48, 144), (0.3, 0.6), (0.15, 0.3))],
    "rsi": [dict(lo=lo, hi=hi, tp=tp, sl=sl, hold=h, range=r, drift=d)
            for lo, hi, tp, sl, h, r, d in itertools.product(
                (20, 30), (50, 65), (0.03, 0.06), (0.05, 0.10), (24, 72), (0.3, 0.6), (0.15, 0.3))],
}


def summarize(rets: list[float]) -> str:
    if not rets:
        return "no trades"
    return (f"n {len(rets):4}  mean {st.mean(rets):+.2%}  median {st.median(rets):+.2%}  "
            f"win {sum(r > 0 for r in rets) / len(rets):.0%}")


def main() -> None:
    uni = universe()
    print(f"universe: {len(uni)} established pools")
    data = []
    for r in uni:
        c = candles(r["pool"])
        if len(c) >= 600:
            data.append((r, Ind(c)))
            print(f"  {r['symbol'][:12]:12} {r['dex']:14} liq ${r['liq'] / 1e3:6.0f}k mcap ${r['mcap'] / 1e6:6.1f}M "
                  f"age {r['age_h'] / 24:5.1f}d bars {len(c)}")
        time.sleep(0.2)
    hours = sum((d.c[-1][0] - d.c[int(len(d.c) * 0.6)][0]) for _r, d in data) / 3600 / max(len(data), 1)
    print(f"\n{len(data)} pools with >= 600 five-minute candles; test window ~{hours:.0f} h each\n")
    for fam, grid in GRIDS.items():
        best = None
        for p in grid:
            tr = []
            for r, d in data:
                tr += simulate(d, 0, int(len(d.c) * 0.6), fam, p, DEX_FEE.get(r["dex"], 0.005))
            if len(tr) < 30 or sum(x > 0 for x in tr) / len(tr) < 0.6:
                continue
            score = st.median(tr) + st.mean(tr)
            if best is None or score > best[0]:
                best = (score, p, tr)
        if not best:
            print(f"{fam}: no parameter set reached 30 trades at >= 60% wins on the first 60%")
            continue
        _s, p, tr = best
        te, per_coin = [], {}
        for r, d in data:
            x = simulate(d, int(len(d.c) * 0.6), len(d.c), fam, p, DEX_FEE.get(r["dex"], 0.005))
            te += x
            if x:
                per_coin[r["symbol"]] = (len(x), st.mean(x))
        tph = len(te) / hours if hours else 0
        pos = sum(1 for n, m in per_coin.values() if m > 0)
        print(f"{fam}: params {p}")
        print(f"   train {summarize(tr)}")
        print(f"   TEST  {summarize(te)}  -> {len(te) / max(hours, 1):.1f} trades/hour per coin-equivalent, "
              f"{tph * len(data):.1f}/hour across all {len(data)} coins; "
              f"{pos}/{len(per_coin)} coins positive")
        for sym, (n, m) in sorted(per_coin.items(), key=lambda x: -x[1][0])[:8]:
            print(f"      {sym[:12]:12} n {n:3} mean {m:+.2%}")


if __name__ == "__main__":
    main()
