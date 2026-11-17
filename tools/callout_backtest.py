"""Can a callout pump be timed? Backtest on the public SmugCalls record (paper research).

SmugCalls posts pump.fun coins to a public Telegram channel the moment its
on-chain filters fire and publishes every call, losers included
(github.com/Smurfetc/solana-memecoin-calls-dataset). For recent calls at or
above MIN_MC market cap, 1-minute GeckoTerminal candles from 15 min before to
60 min after the call are fetched and these are tested after costs:

- buy at the open of minute D after the call (D = 0..10; minute 0 is the
  minute after the call minute, i.e. up to ~60 s late), sell at the close of
  minute D+H (H = 1..30);
- the same entries with a take-profit / stop-loss and a 30-minute cap.

Rules are picked on the older 60% of calls and scored on the newer 40%. Also
reported: the run-up in the 5 minutes before the call and the minute the
post-call high is reached. Minute candles cannot show 10-20 s holds; that
needs trade-by-trade data.

Run from the repo root: python tools/callout_backtest.py [min_mc] [max_calls]
"""

from __future__ import annotations

import json
import statistics as st
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trending import feeds  # noqa: E402

CALLS = "https://raw.githubusercontent.com/Smurfetc/solana-memecoin-calls-dataset/main/calls.jsonl"
MIN_MC = float(sys.argv[1]) if len(sys.argv) > 1 else 20_000
MAX_CALLS = int(sys.argv[2]) if len(sys.argv) > 2 else 400
SIDE = 0.02  # pump.fun fee ~1% + slippage/priority on a fast-moving curve, per side
DELAYS = (0, 1, 2, 3, 5, 10)
HOLDS = (1, 2, 3, 5, 10, 15, 30)
TPSL = [(tp, sl) for tp in (0.1, 0.2, 0.3, 0.5, 1.0) for sl in (0.1, 0.2, 0.3)]


def load_calls() -> list[dict]:
    with urllib.request.urlopen(CALLS, timeout=60) as r:  # noqa: S310
        rows = [json.loads(line) for line in r.read().decode().splitlines() if line.strip()]
    rows = [c for c in rows if c["mc"] >= MIN_MC]
    return sorted(rows, key=lambda c: c["t"])[-MAX_CALLS:]


def pool_for(mint: str) -> str | None:
    doc = feeds.get_json(f"{feeds.GT}/networks/solana/tokens/{mint}/pools?page=1", feeds.GT_GATE)
    pools = (doc or {}).get("data") or []
    # The bonding curve trades first; prefer it, then the oldest pool.
    best = None
    for p in pools:
        a = p.get("attributes") or {}
        dex = (((p.get("relationships") or {}).get("dex") or {}).get("data") or {}).get("id", "")
        created = a.get("pool_created_at") or ""
        key = (0 if "pump-fun" in dex or "pumpfun" in dex else 1, created)
        if best is None or key < best[0]:
            best = (key, a.get("address"))
    return best[1] if best else None


def minutes(pool: str, t_call: int) -> dict[int, tuple]:
    """Minute candles keyed by minute offset from the call minute."""
    doc = feeds.get_json(
        f"{feeds.GT}/networks/solana/pools/{pool}/ohlcv/minute?aggregate=1&limit=80"
        f"&before_timestamp={t_call + 3660}&currency=usd", feeds.GT_GATE)
    rows = (((doc or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
    base = t_call - t_call % 60
    return {int((t - base) // 60): (o, h, lo, c) for t, o, h, lo, c, _v in rows if c}


def fill_forward(m: dict[int, tuple], k: int) -> tuple | None:
    """Candle at offset k, or the last close before it as a flat candle (no trades that minute)."""
    if k in m:
        return m[k]
    prev = [j for j in m if j < k]
    if not prev:
        return None
    c = m[max(prev)][3]
    return (c, c, c, c)


def fixed_hold(m: dict, d: int, h: int) -> float | None:
    e, x = fill_forward(m, 1 + d), fill_forward(m, 1 + d + h)
    if not e or not x or e[0] <= 0:
        return None
    return x[3] / e[0] * (1 - SIDE) ** 2 - 1


def tp_sl(m: dict, d: int, tp: float, sl: float, cap: int = 30) -> float | None:
    e = fill_forward(m, 1 + d)
    if not e or e[0] <= 0:
        return None
    entry = e[0]
    for k in range(1 + d, 1 + d + cap):
        c = fill_forward(m, k)
        if not c:
            return None
        if c[2] <= entry * (1 - sl):
            return (1 - sl) * (1 - SIDE) ** 2 - 1
        if c[1] >= entry * (1 + tp):
            return (1 + tp) * (1 - SIDE) ** 2 - 1
    last = fill_forward(m, 1 + d + cap)
    return last[3] / entry * (1 - SIDE) ** 2 - 1 if last else None


def fmt(x: list[float]) -> str:
    if not x:
        return "n 0"
    return f"n {len(x):3} mean {st.mean(x):+6.1%} median {st.median(x):+6.1%} win {sum(v > 0 for v in x) / len(x):4.0%}"


def main() -> None:
    calls = load_calls()
    print(f"{len(calls)} calls with mc >= ${MIN_MC:,.0f}: {calls[0]['utc']} .. {calls[-1]['utc']}")
    data = []
    for c in calls:
        pool = pool_for(c["mint"])
        if not pool:
            continue
        m = minutes(pool, c["t"])
        if 1 in m or 2 in m:
            data.append((c, m))
        time.sleep(0.1)
    print(f"{len(data)} calls with minute candles after the call")
    if len(data) < 30:
        return
    pre = [fill_forward(m, 0)[3] / fill_forward(m, -5)[3] - 1 for _c, m in data
           if fill_forward(m, -5) and fill_forward(m, 0)]
    peak_min = []
    for _c, m in data:
        after = {k: v for k, v in m.items() if 1 <= k <= 60}
        if after:
            peak_min.append(max(after, key=lambda k: after[k][1]))
    print(f"run-up in the 5 min before the call: median {st.median(pre):+.1%}; "
          f"post-call high reached at minute: median {st.median(peak_min)}, "
          f"share within 2 min {sum(p <= 2 for p in peak_min) / len(peak_min):.0%}")
    cut = int(len(data) * 0.6)
    train, test = data[:cut], data[cut:]
    print(f"\nFixed holds (buy minute D, sell after H min), {len(train)} older / {len(test)} newer calls:")
    best = None
    for d in DELAYS:
        row = []
        for h in HOLDS:
            tr = [r for c, m in train if (r := fixed_hold(m, d, h)) is not None]
            row.append(f"H{h}:{st.median(tr):+.0%}/{st.mean(tr):+.0%}" if tr else f"H{h}:-")
            if len(tr) >= 30 and (best is None or st.mean(tr) > best[0]):
                best = (st.mean(tr), ("hold", d, h), tr)
        print(f"  D{d:<2} train median/mean " + "  ".join(row))
    for d in DELAYS:
        for tp, sl in TPSL:
            tr = [r for c, m in train if (r := tp_sl(m, d, tp, sl)) is not None]
            if len(tr) >= 30 and st.mean(tr) > best[0]:
                best = (st.mean(tr), ("tpsl", d, tp, sl), tr)
    rule = best[1]
    if rule[0] == "hold":
        te = [r for c, m in test if (r := fixed_hold(m, rule[1], rule[2])) is not None]
    else:
        te = [r for c, m in test if (r := tp_sl(m, rule[1], rule[2], rule[3])) is not None]
    print(f"\nBest on the older calls: {rule}\n  train {fmt(best[2])}\n  TEST  {fmt(te)}")
    print("\nNewer calls, a few fixed rules for reference:")
    for d, h in ((0, 1), (0, 3), (0, 10), (1, 2), (2, 5), (5, 10), (10, 20)):
        te = [r for c, m in test if (r := fixed_hold(m, d, h)) is not None]
        print(f"  buy minute {d:2}, sell after {h:2} min: {fmt(te)}")
    for tp, sl in ((0.2, 0.2), (0.3, 0.2), (0.5, 0.3), (1.0, 0.3)):
        te = [r for c, m in test if (r := tp_sl(m, 0, tp, sl)) is not None]
        print(f"  buy minute 0, +{tp:.0%} target / -{sl:.0%} stop: {fmt(te)}")


if __name__ == "__main__":
    main()
