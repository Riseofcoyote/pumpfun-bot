"""Print how many PumpSwap top-volume pools the trending discovery adds, by age band.

Read-only: public GeckoTerminal only. Run via the Research workflow.
"""

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from trending import feeds  # noqa: E402

base = feeds.discover()
wide = feeds.discover(2)
print("pools base", len(base), "with pumpswap pages", len(wide))
new = [r for p, r in wide.items() if p not in base]
print("added", len(new))
band = Counter(
    "4-12h" if 4 <= r["age_h"] <= 12 else ("<4h" if r["age_h"] < 4 else ">12h")
    for r in new
)
print("added by age", dict(band))
for r in sorted(wide.values(), key=lambda r: r["ranks"].get("pumpvol", 99))[:40]:
    print(
        f"{r['symbol']:12} {r['dex']:10} age {r['age_h']:8.1f}h liq ${r['liq']:>10,.0f} "
        f"h24 {r['pc']['h24']:+.0%} ranks {r['ranks']}"
    )
