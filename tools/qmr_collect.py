"""Keep collecting live Solana meme data for quant-memcoins-researcher, then test it.

The first one-shot run (tools/qmr_zoo.py) only gathered about an hour of data and
its candle backfill hit the job timeout, so the strategy zoo never ran. This does
what the tool's own `run` mode does, one pass per workflow run: snapshot pools,
then refresh 5-minute candles for the stalest pools (one call returns ~3.5 days).
The database is carried between runs in the Actions cache (.github/workflows/qmr-collect.yml).

MODE=collect (default): one collect + backfill pass, each step allowed to fail.
MODE=zoo: run every strategy in the library through the tool's gates and print the report.
Read-only and paper only: public market data, no wallet, no secrets, no execution.
"""
import os
import subprocess
import sys
import time

PIN = "f888e977833d26e2baa049d08baabd1634317203"
PKG = f"git+https://github.com/zostaff/quant-memcoins-researcher@{PIN}"
DB = "qmr.db"
MODE = os.environ.get("MODE", "collect")
start = time.time()

subprocess.run([sys.executable, "-m", "pip", "install", "-q", PKG], check=True)
meme = [sys.executable, "-m", "ai_quant_lab.meme"]

if MODE == "collect":
    from ai_quant_lab.meme.geckoterminal import (GeckoClient, backfill_ohlcv, collect_once,
                                                 connect, stale_pools)
    conn, client = connect(DB), GeckoClient()
    try:
        s = collect_once(conn, client, network="solana", pages=5, holder_calls=20)
        print(f"[collect] +{s.new_pools} pools, {s.snapshots} snapshots, "
              f"{s.token_snapshots} token snapshots  (t+{time.time() - start:.0f}s)", flush=True)
    except Exception as e:  # noqa: BLE001 - keep whatever was collected
        print(f"[collect] failed: {e!r}", flush=True)
    total = 0
    while time.time() - start < 20 * 60:  # stay well inside the job timeout
        pools = stale_pools(conn, limit=10, stale_hours=3.0, network="solana")
        if not pools:
            break
        try:
            n = backfill_ohlcv(conn, client, addresses=pools)
        except Exception as e:  # noqa: BLE001
            print(f"[backfill] failed: {e!r}", flush=True)
            break
        total += n
        print(f"[backfill] {n} candles from {len(pools)} pools  (t+{time.time() - start:.0f}s)", flush=True)
    print(f"[backfill] {total} candles this run", flush=True)
    conn.close()
    subprocess.run([*meme, "info", "--db", DB, "--network", "solana"])
else:
    subprocess.run([*meme, "info", "--db", DB, "--network", "solana"])
    subprocess.run([*meme, "zoo", "--db", DB, "--network", "solana", "--out", "zoo_report.md",
                    "--json", "zoo_verdicts.json"], timeout=100 * 60)
    print("\n=== ZOO REPORT")
    try:
        print(open("zoo_report.md").read())
    except OSError as e:
        print(f"no report: {e}")
    print("=== END")
