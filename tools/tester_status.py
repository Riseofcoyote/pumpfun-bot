"""Print the latest [status] lines and warnings from each paper tester's log.

Read-only; runs on the Fly machine through the Fly research workflow:
``tester_status.py [lines]``. Prints only small tails so the job log stays short.
"""

import sys
from collections import deque
from pathlib import Path

ROOT = Path("/data/trades")
N = int(sys.argv[1]) if len(sys.argv) > 1 else 8


def tail(path: Path, needle: str, n: int) -> list[str]:
    """Last ``n`` lines of ``path`` containing ``needle``."""
    out: deque[str] = deque(maxlen=n)
    with path.open(errors="replace") as f:
        for line in f:
            if needle in line:
                out.append(line.rstrip()[:600])
    return list(out)


for d in sorted(p for p in ROOT.iterdir() if p.is_dir()):
    log = d / "engine.log"
    if not log.exists():
        continue
    print(f"=== {d.name}  ({log.stat().st_size // 1024} KB)")
    for line in tail(log, "[status]", N):
        print("  ", line)
    for line in tail(log, "WARNING", 3) + tail(log, "ERROR", 3):
        print("  !", line)
    if d.name == "trending":
        for line in tail(log, "[paper]", 6):
            print("  ", line)
