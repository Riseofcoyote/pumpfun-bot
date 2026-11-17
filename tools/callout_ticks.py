"""Second-level callout timing on pump.fun bonding curves (paper research).

For recent SmugCalls calls (>= MIN_MC market cap) whose coin was still on its
bonding curve, every trade from 60 s before to 10 min after the call is rebuilt
from the curve's transactions (pump.fun TradeEvent logs carry the curve's
virtual reserves after each trade, which give the exact price). Then:

- buy D seconds after the call (D = 0, 5, 10, 20, 30, 60), at the first trade
  price at or after that moment, and sell H seconds later (H = 10, 15, 20, 30,
  60, 120, 300) at the last trade price before then;
- costs: pump.fun 1% fee per side plus price impact on a 0.1 SOL order from the
  curve's reserves, plus 0.5% per side for landing behind other buyers.

Public RPC only, read-only. Run: python tools/callout_ticks.py [min_mc] [max_calls]
"""

from __future__ import annotations

import base64
import json
import os
import statistics as st
import struct
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

CALLS = "https://raw.githubusercontent.com/Smurfetc/solana-memecoin-calls-dataset/main/calls.jsonl"
RPC = os.environ.get("SOLANA_NODE_RPC_ENDPOINT") or "https://api.mainnet-beta.solana.com"
PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
TRADE_EVENT = bytes([189, 219, 127, 211, 78, 230, 97, 238])  # anchor event discriminator
MIN_MC = float(sys.argv[1]) if len(sys.argv) > 1 else 20_000
MAX_CALLS = int(sys.argv[2]) if len(sys.argv) > 2 else 30
FEE = 0.01
LAG = 0.005
SIZE_SOL = 0.1
DELAYS = (0, 5, 10, 20, 30, 60)
HOLDS = (10, 15, 20, 30, 60, 120, 240)
WORKERS = 12
MAX_TXS = 700  # per call, from the call onward
MAX_PAGES = 6  # 6,000 signatures back from now; busier coins are skipped


def rpc(method: str, params: list, tries: int = 6) -> dict | None:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for k in range(tries):
        try:
            req = urllib.request.Request(RPC, body, {"content-type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310
                doc = json.load(r)
            if "error" in doc and doc["error"].get("code") == 429:
                raise OSError("429")
            return doc.get("result")
        except (OSError, ValueError):
            time.sleep(1.5 * (k + 1))
        except Exception:  # noqa: BLE001
            time.sleep(1.5 * (k + 1))
    return None


def curve_pda(mint: str) -> str:
    from solders.pubkey import Pubkey  # noqa: PLC0415

    pda, _ = Pubkey.find_program_address([b"bonding-curve", bytes(Pubkey.from_string(mint))],
                                         Pubkey.from_string(PUMP))
    return str(pda)


def sigs_around(curve: str, t0: int, t1: int) -> list[str]:
    """Signatures on the curve with blockTime in [t0, t1], paging back from now."""
    out, before = [], None
    for _ in range(MAX_PAGES):
        opts = {"limit": 1000} | ({"before": before} if before else {})
        page = rpc("getSignaturesForAddress", [curve, opts]) or []
        if not page:
            break
        for s in page:
            bt = s.get("blockTime") or 0
            if t0 <= bt <= t1 and s.get("err") is None:
                out.append(s["signature"])
        before = page[-1]["signature"]
        if (page[-1].get("blockTime") or 0) < t0:
            break
    return out


def trades(sig: str, mint: str) -> list[tuple[float, float, float]]:
    """(timestamp, virtual SOL, virtual tokens) after each TradeEvent for mint in tx."""
    tx = rpc("getTransaction", [sig, {"maxSupportedTransactionVersion": 0, "encoding": "json"}])
    out = []
    for line in ((tx or {}).get("meta") or {}).get("logMessages") or []:
        if not line.startswith("Program data: "):
            continue
        try:
            raw = base64.b64decode(line[14:])
        except ValueError:
            continue
        if raw[:8] != TRADE_EVENT or len(raw) < 8 + 32 + 8 + 8 + 1 + 32 + 8 + 16:
            continue
        o = 8
        ev_mint = raw[o:o + 32]
        o += 32 + 8 + 8 + 1 + 32
        ts, vsol, vtok = struct.unpack_from("<qQQ", raw, o)
        from solders.pubkey import Pubkey  # noqa: PLC0415

        if str(Pubkey.from_bytes(ev_mint)) == mint and vtok > 0:
            out.append((float(ts), vsol / 1e9, vtok / 1e6))
    return out


def buy_tokens(vsol: float, vtok: float, sol: float) -> float:
    s = sol * (1 - FEE)
    return vtok - vsol * vtok / (vsol + s)


def sell_sol(vsol: float, vtok: float, tok: float) -> float:
    return (vsol - vsol * vtok / (vtok + tok)) * (1 - FEE)


def table(results: dict, used: int) -> None:
    print(f"\n--- after {used} calls: buy +Ds after call, sell Hs later: median / mean / win%", flush=True)
    for d in DELAYS:
        cells = []
        for h in HOLDS:
            x = results.get((d, h), [])
            cells.append(f"{h:>3}s {st.median(x):+5.0%}/{st.mean(x):+5.0%}/{sum(v > 0 for v in x) / len(x):3.0%}"
                         if x else f"{h:>3}s -")
        print(f"  D{d:<3} " + " | ".join(cells), flush=True)


def main() -> None:
    print(f"RPC: {'plan endpoint' if os.environ.get('SOLANA_NODE_RPC_ENDPOINT') else 'public mainnet-beta'}", flush=True)
    with urllib.request.urlopen(CALLS, timeout=60) as r:  # noqa: S310
        rows = [json.loads(x) for x in r.read().decode().splitlines() if x.strip()]
    rows = sorted((c for c in rows if c["mc"] >= MIN_MC and c["mint"].endswith("pump")),
                  key=lambda c: c["t"])[-MAX_CALLS:]
    print(f"{len(rows)} recent calls >= ${MIN_MC:,.0f}: {rows[0]['utc']} .. {rows[-1]['utc']}")
    results: dict[tuple, list[float]] = {}
    used = 0
    for c in rows:
        sigs = sigs_around(curve_pda(c["mint"]), c["t"] - 30, c["t"] + 330)
        sigs.reverse()  # oldest first: the trades right at the call matter most
        print(f"  {c['sym'][:10]:10} {len(sigs)} curve txs in the window", flush=True)
        with ThreadPoolExecutor(WORKERS) as pool:
            got = pool.map(lambda sg: trades(sg, c["mint"]), sigs[:MAX_TXS])
            path = sorted(t for ts in got for t in ts)
        if len(path) < 5:
            continue
        used += 1
        before_call = [p for p in path if p[0] <= c["t"]]
        print(f"  {c['sym'][:10]:10} mc ${c['mc']:,.0f}  trades {len(path):4}  "
              f"in first 60 s {sum(1 for p in path if c['t'] < p[0] <= c['t'] + 60):3}  "
              f"price 60 s after vs call {((next((p for p in path if p[0] >= c['t'] + 60), path[-1])[1] / next((p for p in path if p[0] >= c['t'] + 60), path[-1])[2]) / (before_call[-1][1] / before_call[-1][2]) - 1) if before_call else float('nan'):+.0%}")
        for d in DELAYS:
            entry = next((p for p in path if p[0] >= c["t"] + d), None)
            if not entry:
                continue
            tok = buy_tokens(entry[1], entry[2], SIZE_SOL) * (1 - LAG)
            for h in HOLDS:
                ex = [p for p in path if p[0] <= entry[0] + h]
                if not ex:
                    continue
                last = ex[-1]
                got = sell_sol(last[1], last[2], tok) * (1 - LAG)
                results.setdefault((d, h), []).append(got / SIZE_SOL - 1)
        if used % 5 == 0:
            table(results, used)
    print(f"\n{used} calls with curve trades around the call")
    table(results, used)


if __name__ == "__main__":
    main()
