"""Run zostaff/quant-memcoins-researcher's strategy zoo on live Solana meme data.

Read-only and paper only: the tool has no wallet and no execution. Installs the
package pinned to one reviewed commit, runs its synthetic demo as a sanity check,
collects Solana pools from GeckoTerminal, backfills their 5-minute candles, then
runs every strategy in its library through the cost model and luck gates and
prints the report. No API key is set, so its Claude-driven `research` mode is off.
"""
import subprocess
import sys
import time

PIN = "f888e977833d26e2baa049d08baabd1634317203"
PKG = f"git+https://github.com/zostaff/quant-memcoins-researcher@{PIN}"
DB = "qmr.db"
start = time.time()


def sh(*args, timeout=None, check=True):
    print(f"\n$ {' '.join(args)}  (t+{time.time() - start:.0f}s)", flush=True)
    r = subprocess.run(args, timeout=timeout)
    if check and r.returncode:
        print(f"exit {r.returncode}", flush=True)
    return r.returncode


meme = [sys.executable, "-m", "ai_quant_lab.meme"]
sh(sys.executable, "-m", "pip", "install", "-q", PKG)
sh(*meme, "zoo", "--list", check=False)
sh(*meme, "demo", "--days", "14", timeout=900, check=False)
for _ in range(3):  # three passes a few minutes apart, so pools get more than one snapshot
    sh(*meme, "collect", "--db", DB, "--network", "solana", "--pages", "5", "--holder-calls", "20",
       timeout=900, check=False)
    sh(*meme, "backfill", "--db", DB, timeout=1800, check=False)
    if time.time() - start > 60 * 60:
        break
sh(*meme, "info", "--db", DB, "--network", "solana", check=False)
sh(*meme, "zoo", "--db", DB, "--network", "solana", "--out", "zoo_report.md", timeout=1800, check=False)
print("\n=== ZOO REPORT")
try:
    print(open("zoo_report.md").read())
except OSError as e:
    print(f"no report: {e}")
print("=== END")
