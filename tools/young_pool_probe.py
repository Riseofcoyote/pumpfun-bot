"""Count 4-12 h Solana pools with $50k+ liquidity that each candidate source lists.

Read-only: public GeckoTerminal, DexScreener and pump.fun endpoints only. Run via
the Research workflow. Finds a discovery source for the trending engine, whose
current lists hold almost no 4-12 h pools that are not ticker relaunches.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from trending import feeds  # noqa: E402

GT = feeds.GT
CANDIDATES = {
    "gt_pumpswap_tx": [
        f"{GT}/networks/solana/dexes/pumpswap/pools?sort=h24_tx_count_desc&page={p}"
        for p in (1, 2, 3)
    ],
    "gt_solana_tx": [
        f"{GT}/networks/solana/pools?sort=h24_tx_count_desc&page={p}" for p in (1, 2, 3)
    ],
    "gt_new_pools": [f"{GT}/networks/solana/new_pools?page={p}" for p in range(1, 11)],
    "gt_pumpswap_vol_p3_5": [
        f"{GT}/networks/solana/dexes/pumpswap/pools?sort=h24_volume_usd_desc&page={p}"
        for p in (3, 4, 5)
    ],
}


def band(age: float) -> str:
    return "4-12h" if 4 <= age <= 12 else ("<4h" if age < 4 else ">12h")


def show(name: str, recs: list[dict]) -> None:
    from collections import Counter

    c = Counter(band(r["age_h"]) for r in recs)
    good = [r for r in recs if 4 <= r["age_h"] <= 12 and r["liq"] >= 50_000]
    print(f"\n== {name}: {len(recs)} pools by age {dict(c)}; 4-12h & liq>=50k: {len(good)}")
    for r in sorted(good, key=lambda r: r["age_h"]):
        print(
            f"  {r['symbol']:14} {r['dex']:12} age {r['age_h']:5.1f}h liq ${r['liq']:>10,.0f}"
            f" mcap ${r['mcap']:>12,.0f} h1 {r['pc']['h1']:+.0%} h24 {r['pc']['h24']:+.0%}"
            f" {r['mint']}"
        )


for name, urls in CANDIDATES.items():
    recs: list[dict] = []
    for u in urls:
        doc = feeds.get_json(u, feeds.GT_GATE, feeds.GT_HDR)
        if not doc:
            print(f"{name}: no data from {u}")
            continue
        recs += [r for p in doc.get("data") or [] if (r := feeds._parse_gt_pool(p))]
    show(name, recs)

# pump.fun's own list of graduated coins, newest first and by market cap.
for name, url in {
    "pumpfun_grad_mcap": "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50"
    "&sort=market_cap&order=DESC&includeNsfw=true&complete=true",
    "pumpfun_grad_new": "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50"
    "&sort=created_timestamp&order=DESC&includeNsfw=true&complete=true",
}.items():
    doc = feeds.get_json(url, feeds.DS_GATE)
    if doc is None:
        print(f"\n== {name}: no data")
        continue
    coins = doc if isinstance(doc, list) else doc.get("coins") or []
    print(f"\n== {name}: {len(coins)} coins")
    import time

    now = time.time()
    for c in coins[:50]:
        ts = (c.get("created_timestamp") or 0) / 1000
        print(
            f"  {str(c.get('symbol'))[:14]:14} age {(now - ts) / 3600:7.1f}h"
            f" usd_mcap {c.get('usd_market_cap') or 0:>12,.0f} {c.get('mint')}"
        )
