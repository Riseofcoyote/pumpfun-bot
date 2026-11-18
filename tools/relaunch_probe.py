"""Show which tickers the trending tester's relaunch block is skipping, and why.

Read-only; runs on the Fly machine through the Fly research workflow:
``relaunch_probe.py [lines]``. Prints the latest "[relaunch] skipping" log lines
and, for each ticker, every mint it was seen on with its last-seen time.
"""

import json
import sys
import time
from collections import deque
from pathlib import Path

D = Path("/data/trades/trending")
N = int(sys.argv[1]) if len(sys.argv) > 1 else 60

skips: deque[str] = deque(maxlen=N)
with (D / "engine.log").open(errors="replace") as f:
    for line in f:
        if "[relaunch] skipping" in line:
            skips.append(line.rstrip()[:200])
print(f"=== last {len(skips)} relaunch skips")
for s in skips:
    print(s)

state = json.loads((D / "state.json").read_text())
sm = state.get("symbol_mints") or {}
now = time.time()
multi = {s: m for s, m in sm.items() if len(m) > 1}
print(f"=== tickers tracked {len(sm)}, on 2+ mints {len(multi)}")
syms = {line.split("skipping ")[1].split()[0].upper() for line in skips}
for sym in sorted(syms):
    mints = sm.get(sym, {})
    seen = ", ".join(
        f"{m[:6]} {(now - t) / 3600:.1f}h ago" for m, t in sorted(mints.items())
    )
    print(f"{sym:14} {len(mints)} mints: {seen}")
