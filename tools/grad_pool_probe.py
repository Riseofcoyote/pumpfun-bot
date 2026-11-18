"""Live check of the trending engine's graduation source (read-only, public APIs).

Uses the mints of PumpSwap's top-volume pools as stand-ins for graduations, then
runs them through ``feeds.pumpswap_pools_of`` and ``feeds.discover(grad_pools=)``
the way ``trending.engine.GradSource`` does. Run via the Research workflow.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from trending import feeds

mints: list[str] = []
for page in (1, 2, 3):
    url = (
        f"{feeds.GT}/networks/solana/dexes/pumpswap/pools"
        f"?sort=h24_tx_count_desc&page={page}"
    )
    doc = feeds.get_json(url, feeds.GT_GATE, feeds.GT_HDR) or {}
    mints += [
        r["mint"] for p in doc.get("data") or [] if (r := feeds._parse_gt_pool(p))
    ]  # noqa: SLF001
print(f"{len(mints)} stand-in mints")
wide = feeds.pumpswap_pools_of(mints, 0, 0, 100_000)
print(f"pumpswap_pools_of, no filters: {len(wide)} pools")
young = feeds.pumpswap_pools_of(mints, 40_000, 3.5, 12)
print(f"pumpswap_pools_of, liq>=40k age 3.5-12h: {len(young)} pools")
snap = feeds.discover(0, wide[:30])
grad = [r for r in snap.values() if "grad" in r["ranks"]]
print(f"discover(grad_pools=30): {len(snap)} pools, {len(grad)} with a grad rank")
for r in sorted(grad, key=lambda r: r["ranks"]["grad"])[:12]:
    print(
        f"  #{r['ranks']['grad']:2} {r['symbol']:12} {r['dex']:9} age {r['age_h']:7.1f}h "
        f"liq ${r['liq']:>10,.0f} h1 {r['pc']['h1']:+.0%} h24 {r['pc']['h24']:+.0%} "
        f"buyers5m {r['tx']['m5']['buyers']}"
    )
