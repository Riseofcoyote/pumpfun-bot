"""Read-only on-chain trade report for the bot wallet.

Pairs each pump.fun buy and sell by mint from the wallet's recent transactions
and prints the net SOL per coin (fees, slippage and reclaimed rent included).
Only coins whose buy cost is under ``MAX_BUY_SOL`` count as bot trades; larger
ones are the owner's manual trades. Needs env SOLANA_NODE_RPC_ENDPOINT and
WALLET; never prints the endpoint.
"""

import json
import os
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

RPC = os.environ["SOLANA_NODE_RPC_ENDPOINT"]
WALLET = os.environ["WALLET"]
LIMIT = int(os.environ.get("LIMIT", "300"))
MAX_BUY_SOL = float(os.environ.get("MAX_BUY_SOL", "0.03"))


def rpc(method: str, params: list) -> dict | None:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    for attempt in range(4):
        try:
            req = urllib.request.Request(
                RPC, body.encode(), {"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=20) as r:
                out = json.loads(r.read())
            if "result" in out:
                return out["result"]
        except Exception as e:  # noqa: BLE001
            print(f"  rpc {method} retry {attempt + 1}: {type(e).__name__}", file=sys.stderr)
        time.sleep(1.5 * (attempt + 1))
    return None


def main() -> None:
    bal = rpc("getBalance", [WALLET])
    print(f"Wallet {WALLET}  balance: {bal['value'] / 1e9:.4f} SOL" if bal else "balance: n/a")
    sigs = rpc("getSignaturesForAddress", [WALLET, {"limit": LIMIT}]) or []
    print(f"Scanning {len(sigs)} most recent transactions\n")

    legs = defaultdict(list)  # mint -> [(time, sol_delta, token_delta, failed)]
    failed_txs = 0
    for s in sigs:
        tx = rpc(
            "getTransaction",
            [s["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}],
        )
        time.sleep(0.12)
        if not tx or not tx.get("meta"):
            continue
        meta = tx["meta"]
        keys = [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"]["accountKeys"]]
        if WALLET not in keys:
            continue
        i = keys.index(WALLET)
        sol_delta = (meta["postBalances"][i] - meta["preBalances"][i]) / 1e9
        if meta.get("err"):
            failed_txs += 1
        pre = {b["mint"]: float(b["uiTokenAmount"]["uiAmount"] or 0)
               for b in meta.get("preTokenBalances", []) if b.get("owner") == WALLET}
        post = {b["mint"]: float(b["uiTokenAmount"]["uiAmount"] or 0)
                for b in meta.get("postTokenBalances", []) if b.get("owner") == WALLET}
        mints = (set(pre) | set(post)) - {"So11111111111111111111111111111111111111112"}
        for m in mints:
            d = post.get(m, 0) - pre.get(m, 0)
            legs[m].append((tx.get("blockTime") or 0, sol_delta, d, bool(meta.get("err"))))
        if not mints and abs(sol_delta) > 0:
            # rent reclaim (close account) or transfer: attach later by time if needed
            pass

    rows = []
    for m, ls in legs.items():
        ls.sort()
        ok = [x for x in ls if not x[3]]
        buys = [x for x in ok if x[2] > 0]
        sells = [x for x in ok if x[2] < 0]
        if not buys:
            continue
        cost = -sum(x[1] for x in buys)
        if cost > MAX_BUY_SOL:
            continue  # manual trade
        proceeds = sum(x[1] for x in sells)
        t0 = buys[0][0]
        t1 = sells[-1][0] if sells else None
        held = sum(x[2] for x in buys) + sum(x[2] for x in sells)
        rows.append({
            "time": datetime.fromtimestamp(t0, timezone.utc).strftime("%m-%d %H:%M:%S"),
            "mint": m,
            "cost": cost,
            "back": proceeds,
            "net": proceeds - cost,
            "hold_s": (t1 - t0) if t1 else None,
            "open": held > 1e-6 and not sells,
            "failed_legs": len(ls) - len(ok),
        })
    rows.sort(key=lambda r: r["time"])

    print(f"{'time UTC':15} {'mint':12} {'cost':>8} {'back':>8} {'net':>9} {'net%':>7} {'hold':>6}  note")
    for r in rows:
        pct = r["net"] / r["cost"] * 100 if r["cost"] else 0
        hold = f"{r['hold_s']}s" if r["hold_s"] is not None else "-"
        note = "STILL HOLDING" if r["open"] else ""
        if r["failed_legs"]:
            note += f" {r['failed_legs']} failed tx"
        print(f"{r['time']:15} {r['mint'][:10]}.. {r['cost']:8.5f} {r['back']:8.5f} "
              f"{r['net']:+9.5f} {pct:+6.1f}% {hold:>6}  {note}")

    closed = [r for r in rows if not r["open"]]
    if closed:
        wins = [r for r in closed if r["net"] > 0]
        nets = [r["net"] / r["cost"] for r in closed if r["cost"]]
        print(f"\nBot trades closed: {len(closed)} | wins {len(wins)} "
              f"({len(wins) / len(closed):.0%}) | net {sum(r['net'] for r in closed):+.5f} SOL")
        print(f"avg {sum(nets) / len(nets):+.1%} per trade | best {max(nets):+.1%} | worst {min(nets):+.1%}")
        holds = sorted(r["hold_s"] for r in closed if r["hold_s"] is not None)
        if holds:
            print(f"hold time median {holds[len(holds) // 2]}s, max {holds[-1]}s")
        buckets = defaultdict(int)
        for n in nets:
            b = "<-30%" if n < -0.3 else "-30..-15%" if n < -0.15 else "-15..0%" if n < 0 else \
                "0..+15%" if n < 0.15 else "+15..+50%" if n < 0.5 else ">+50%"
            buckets[b] += 1
        print("result spread:", dict(buckets))
    print(f"failed transactions in window: {failed_txs}")


if __name__ == "__main__":
    main()
