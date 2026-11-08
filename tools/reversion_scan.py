"""Deviation-reversion backtest on large established Solana coins (paper research).

The pattern scan found ZEC and PUMP strongly mean-reverting over 15 min to 1 h
(variance ratio 0.53-0.70, 5-minute autocorrelation -0.20), but their typical
5-minute move is smaller than a round trip's cost. This tests bigger dips:
buy when the 5-minute close is d below its n-bar average, sell back at the
average (or +d), stop at -2d, give up after `hold` bars. Parameters are picked
per coin and also globally on the first 60% of candles and scored on the last
40%, at two cost levels per side (0.25%: low priority fee on a big pool;
0.45%: the default). Universe: top 12 established pools by 24 h volume.

Run from the repo root: python tools/reversion_scan.py
"""

from __future__ import annotations

import itertools
import statistics as st
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pattern_scan import ohlcv, top_pools  # noqa: E402

GRID = list(itertools.product((12, 24, 48), (0.015, 0.02, 0.03, 0.05), (24, 72)))


def run(c: list, start: int, end: int, n: int, d: float, hold: int, cost: float) -> list[float]:
    closes = [r[4] for r in c]
    out, i = [], max(start, n)
    while i < end - 1:
        m = sum(closes[i - n:i]) / n
        if closes[i] < m * (1 - d):
            e = c[i + 1][1]
            tp, sl = max(m, e * (1 + d)), e * (1 - 2 * d)
            px, j = None, i + 1
            while j < min(len(c), i + 1 + hold):
                if c[j][3] <= sl:
                    px = sl
                    break
                if c[j][2] >= tp:
                    px = tp
                    break
                j += 1
            if px is None:
                j = min(len(c) - 1, i + hold)
                px = c[j][4]
            out.append(px / e * (1 - cost) ** 2 - 1)
            i = j + 1
        else:
            i += 1
    return out


def fmt(x: list[float]) -> str:
    if not x:
        return "no trades"
    return f"n {len(x):3} mean {st.mean(x):+.2%} median {st.median(x):+.2%} win {sum(v > 0 for v in x) / len(x):.0%}"


def main() -> None:
    data = []
    for r in top_pools(12):
        c = ohlcv(r["pool"], "minute", 5)
        time.sleep(0.5)
        if len(c) >= 600:
            data.append((r, c))
    hours = st.mean((c[-1][0] - c[int(len(c) * 0.6)][0]) / 3600 for _r, c in data)
    print(f"{len(data)} coins, test window ~{hours:.0f} h each: " + ", ".join(r["symbol"] for r, _ in data))
    for cost in (0.0025, 0.0045):
        print(f"\n##### cost {cost:.2%} per side")
        best = None
        for g in GRID:
            tr = [x for _r, c in data for x in run(c, 0, int(len(c) * 0.6), *g, cost)]
            if len(tr) >= 20 and (best is None or st.mean(tr) > best[0]):
                best = (st.mean(tr), g, tr)
        if best:
            te_all, pos = [], 0
            for _r, c in data:
                x = run(c, int(len(c) * 0.6), len(c), *best[1], cost)
                te_all += x
                pos += bool(x) and st.mean(x) > 0
            print(f"GLOBAL {best[1]} (n, dip, hold): train {fmt(best[2])}")
            print(f"       TEST {fmt(te_all)}  ~{len(te_all) / hours:.1f} trades/hour across all coins, "
                  f"{pos}/{len(data)} coins positive")
        print("per coin (params picked on that coin's first 60%):")
        for r, c in data:
            cut = int(len(c) * 0.6)
            pick = None
            for g in GRID:
                tr = run(c, 0, cut, *g, cost)
                if len(tr) >= 6 and (pick is None or st.mean(tr) > pick[0]):
                    pick = (st.mean(tr), g, tr)
            if not pick:
                print(f"   {r['symbol'][:10]:10} too few dips")
                continue
            te = run(c, cut, len(c), *pick[1], cost)
            print(f"   {r['symbol'][:10]:10} {str(pick[1]):18} train {fmt(pick[2])} | TEST {fmt(te)}")


if __name__ == "__main__":
    main()
