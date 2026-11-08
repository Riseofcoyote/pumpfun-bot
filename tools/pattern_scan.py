"""Per-coin pattern scan on the top established Solana coins (paper research only).

Top 5 pools by 24 h volume that are 7+ days old with $500k+ liquidity. For each:
5-minute candles (up to 1000, ~3.5 days) and 1-hour candles (up to 1000, ~41 days)
from GeckoTerminal, then
- variance ratios (VR < 1: moves tend to reverse; VR > 1: moves tend to continue)
  and lag-1 autocorrelation at 5 min and 1 h,
- hour-of-day mean 1 h return with a t-stat (UTC hours),
- simple per-coin rules after costs, chosen on the first 60% and scored on the
  last 40%: buy after k red 5-minute candles in a row (reversal), buy a break of
  the prior 4 h high (momentum), and hold through the best hour of the day.

Run from the repo root: python tools/pattern_scan.py
"""

from __future__ import annotations

import statistics as st
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trending import feeds  # noqa: E402

SIDE_COST = {"pumpswap": 0.005, "raydium": 0.004, "raydium-clmm": 0.004, "orca": 0.004,
             "meteora": 0.006}  # dex fee + slippage + network, per side, on a 0.1 SOL order


def top_pools(n: int = 5) -> list[dict]:
    pools = dict(feeds.discover())
    for page in (1, 2, 3):
        doc = feeds.get_json(f"{feeds.GT}/networks/solana/pools?page={page}&sort=h24_volume_usd_desc",
                             feeds.GT_GATE)
        for p in (doc or {}).get("data") or []:
            rec = feeds._parse_gt_pool(p)
            if rec:
                pools.setdefault(rec["pool"], rec)
    best: dict[str, dict] = {}
    for r in pools.values():
        if r["age_h"] < 168 or r["liq"] < 500_000:
            continue
        if r["symbol"].upper() in ("SOL", "WSOL", "USDC", "USDT", "JITOSOL", "MSOL", "JUPSOL", "BSOL"):
            continue
        prev = best.get(r["mint"])
        if prev is None or r["vol"]["h24"] > prev["vol"]["h24"]:
            best[r["mint"]] = r
    return sorted(best.values(), key=lambda r: -r["vol"]["h24"])[:n]


def ohlcv(pool: str, tf: str, agg: int) -> list[tuple]:
    doc = feeds.get_json(
        f"{feeds.GT}/networks/solana/pools/{pool}/ohlcv/{tf}?aggregate={agg}&limit=1000&currency=usd",
        feeds.GT_GATE)
    rows = (((doc or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
    return sorted((float(t), float(o), float(h), float(lo), float(c)) for t, o, h, lo, c, _v in rows if c)


def rets(c: list) -> list[float]:
    return [c[i][4] / c[i - 1][4] - 1 for i in range(1, len(c)) if c[i - 1][4] > 0]


def autocorr(x: list[float]) -> float:
    m = st.mean(x)
    num = sum((x[i] - m) * (x[i - 1] - m) for i in range(1, len(x)))
    den = sum((v - m) ** 2 for v in x)
    return num / den if den else 0.0


def var_ratio(c: list, q: int) -> float:
    r1 = rets(c)
    rq = [c[i][4] / c[i - q][4] - 1 for i in range(q, len(c), q)]
    v1 = st.pvariance(r1)
    return st.pvariance(rq) / (q * v1) if v1 and len(rq) > 5 else float("nan")


def red_run(c: list, start: int, end: int, k: int, tp: float, sl: float, hold: int, cost: float) -> list[float]:
    out, i = [], max(start, k)
    while i < end - 1:
        if all(c[j][4] < c[j][1] for j in range(i - k + 1, i + 1)):
            e = c[i + 1][1]
            px, j = None, i + 1
            while j < min(len(c), i + 1 + hold):
                if c[j][3] <= e * (1 - sl):
                    px = e * (1 - sl)
                    break
                if c[j][2] >= e * (1 + tp):
                    px = e * (1 + tp)
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


def breakout(c: list, start: int, end: int, look: int, tp: float, sl: float, hold: int, cost: float) -> list[float]:
    out, i = [], max(start, look)
    while i < end - 1:
        prior = max(r[2] for r in c[i - look:i])
        if c[i][4] > prior:
            e = c[i + 1][1]
            px, j = None, i + 1
            while j < min(len(c), i + 1 + hold):
                if c[j][3] <= e * (1 - sl):
                    px = e * (1 - sl)
                    break
                if c[j][2] >= e * (1 + tp):
                    px = e * (1 + tp)
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


def best_on_train(fn, c: list, grid: list[tuple], cost: float) -> tuple | None:
    cut = int(len(c) * 0.6)
    best = None
    for g in grid:
        tr = fn(c, 0, cut, *g, cost)
        if len(tr) < 10:
            continue
        sc = st.mean(tr)
        if best is None or sc > best[0]:
            best = (sc, g, tr)
    if not best:
        return None
    te = fn(c, cut, len(c), *best[1], cost)
    return best[1], best[2], te


def fmt(x: list[float]) -> str:
    if not x:
        return "no trades"
    return f"n {len(x):3} mean {st.mean(x):+.2%} median {st.median(x):+.2%} win {sum(v > 0 for v in x) / len(x):.0%}"


def main() -> None:
    for r in top_pools():
        cost = SIDE_COST.get(r["dex"], 0.01)
        m5 = ohlcv(r["pool"], "minute", 5)
        h1 = ohlcv(r["pool"], "hour", 1)
        time.sleep(0.5)
        print(f"\n=== {r['symbol']}  {r['dex']}  liq ${r['liq'] / 1e6:.1f}M  mcap ${r['mcap'] / 1e6:.0f}M  "
              f"vol24 ${r['vol']['h24'] / 1e6:.1f}M  age {r['age_h'] / 24:.0f}d  "
              f"({len(m5)} x 5m, {len(h1)} x 1h)  round-trip cost ~{2 * cost:.1%}")
        if len(m5) < 300 or len(h1) < 200:
            print("   not enough candles")
            continue
        r5, r1h = rets(m5), rets(h1)
        print(f"   volatility: 5m {st.pstdev(r5):.2%}  1h {st.pstdev(r1h):.2%}  "
              f"typical daily range {st.median(max(x[2] for x in h1[i:i + 24]) / min(x[3] for x in h1[i:i + 24]) - 1 for i in range(0, len(h1) - 24, 24)):.1%}")
        print(f"   behaviour: autocorr 5m {autocorr(r5):+.2f}  1h {autocorr(r1h):+.2f} | "
              f"variance ratio 15m {var_ratio(m5, 3):.2f}  1h {var_ratio(m5, 12):.2f}  1d {var_ratio(h1, 24):.2f}")
        by_hour: dict[int, list[float]] = {}
        for i in range(1, len(h1)):
            hr = time.gmtime(h1[i][0]).tm_hour
            by_hour.setdefault(hr, []).append(h1[i][4] / h1[i - 1][4] - 1)
        stats = []
        for hr, xs in by_hour.items():
            if len(xs) >= 10:
                t = st.mean(xs) / (st.pstdev(xs) / len(xs) ** 0.5) if st.pstdev(xs) else 0
                stats.append((t, hr, st.mean(xs), len(xs)))
        stats.sort()
        if stats:
            lo, hi = stats[0], stats[-1]
            print(f"   time of day (UTC): weakest hour {lo[1]:02d}:00 mean {lo[2]:+.2%} (t {lo[0]:+.1f}, n {lo[3]}) | "
                  f"strongest {hi[1]:02d}:00 mean {hi[2]:+.2%} (t {hi[0]:+.1f}, n {hi[3]})")
            # Hold through the strongest hour, picked on the first 60% of days only.
            cut = h1[int(len(h1) * 0.6)][0]
            tr_h = {}
            for i in range(1, len(h1)):
                if h1[i][0] < cut:
                    tr_h.setdefault(time.gmtime(h1[i][0]).tm_hour, []).append(h1[i][4] / h1[i - 1][4] - 1)
            if tr_h:
                pick = max(tr_h, key=lambda k: st.mean(tr_h[k]))
                te = [h1[i][4] / h1[i - 1][4] * (1 - cost) ** 2 - 1 for i in range(1, len(h1))
                      if h1[i][0] >= cut and time.gmtime(h1[i][0]).tm_hour == pick]
                print(f"   best-hour rule (hour {pick:02d} UTC picked on first 60%): TEST {fmt(te)}")
        rr = best_on_train(red_run, m5, [(k, tp, sl, h) for k in (3, 4, 5) for tp in (0.01, 0.02, 0.04)
                                         for sl in (0.02, 0.04) for h in (12, 36)], cost)
        if rr:
            print(f"   reversal (k red 5m candles) {rr[0]}: train {fmt(rr[1])} | TEST {fmt(rr[2])}")
        bo = best_on_train(breakout, m5, [(lk, tp, sl, h) for lk in (24, 48) for tp in (0.02, 0.04, 0.08)
                                          for sl in (0.02, 0.04) for h in (12, 36)], cost)
        if bo:
            print(f"   breakout (prior high) {bo[0]}: train {fmt(bo[1])} | TEST {fmt(bo[2])}")


if __name__ == "__main__":
    main()
