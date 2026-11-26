"""Copy-trade study, wider search part 3: wallets that bought the trending coins early.

Read-only. For each coin in data/copy_candidates.json that launched in the last
30 days, pages its on-chain signatures back to launch, then parses transactions in
time order and records each fee payer that bought (token up, SOL down) between 60 s
after launch (later than snipers) and the time its trending pool was created, or the
first 2 hours when the pool is as old as the coin. Wallets early on 2+ coins, plus
the biggest early buyers that also sold at a profit, become candidates for the
30-day scorer. Prints JSON between markers.
"""
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from copy_wallets import rpc, jup_prices, sol_usd, WSOL, USD_MINTS  # noqa: E402

MAX_PAGES = 400          # 400k signatures
MAX_PARSE = 2500         # transactions parsed per coin, oldest first
SNIPE_S = 60
FALLBACK_WINDOW_S = 2 * 3600


def ts(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() if iso else None


def all_sigs(mint):
    sigs, before = [], None
    for _ in range(MAX_PAGES):
        p = {"limit": 1000}
        if before:
            p["before"] = before
        page = rpc("getSignaturesForAddress", [mint, p]) or []
        if not page:
            break
        sigs += page
        before = page[-1]["signature"]
        if len(page) < 1000:
            return sigs, True
    return sigs, False


def payer_trade(tx, mint, solusd):
    meta = tx.get("meta") or {}
    if meta.get("err"):
        return None
    keys = [k if isinstance(k, str) else k.get("pubkey") for k in tx["transaction"]["message"]["accountKeys"]]
    w = keys[0]
    sol = (meta["postBalances"][0] - meta["preBalances"][0]) / 1e9
    tok, other = 0.0, 0.0
    for side, sign in (("preTokenBalances", -1), ("postTokenBalances", 1)):
        for b in meta.get(side) or []:
            if b.get("owner") != w:
                continue
            amt = float(b["uiTokenAmount"].get("uiAmountString") or 0)
            if b["mint"] == mint:
                tok += sign * amt
            elif b["mint"] == WSOL:
                sol += sign * amt
            elif b["mint"] in USD_MINTS:
                sol += sign * amt / solusd
    if tok > 0 and sol < -0.001:
        return w, "buy", -sol, tok
    if tok < 0 and sol > 0.0005:
        return w, "sell", sol, -tok
    return None


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    cand = json.load(open(os.path.join(here, "data", "copy_candidates.json")))
    solusd = sol_usd()
    mints = [c["mint"] for c in cand["coins"]]
    info = jup_prices(mints)
    now = time.time()
    early = defaultdict(dict)  # wallet -> mint -> stats
    for c in cand["coins"]:
        created = ts((info.get(c["mint"]) or {}).get("createdAt"))
        pool_t = ts(c.get("created"))
        if not created or now - created > 30 * 86400:
            print(f"{c['sym']}: launched {'unknown' if not created else f'{(now - created) / 86400:.0f} days ago'}, skipped")
            continue
        t0 = time.time()
        sigs, complete = all_sigs(c["mint"])
        sigs = [s for s in sigs if not s.get("err")]
        sigs.sort(key=lambda s: (s.get("blockTime") or 0, s.get("slot") or 0))
        if not complete:
            print(f"{c['sym']}: over {MAX_PAGES}k signatures, cannot reach launch, skipped")
            continue
        start = sigs[0].get("blockTime") or created
        end = pool_t if pool_t and pool_t - start > 600 else start + FALLBACK_WINDOW_S
        window = [s for s in sigs if start + SNIPE_S <= (s.get("blockTime") or 0) <= end][:MAX_PARSE]
        buys = 0
        for s in window:
            tx = rpc("getTransaction", [s["signature"], {"maxSupportedTransactionVersion": 0, "encoding": "json"}])
            if not tx:
                continue
            r = payer_trade(tx, c["mint"], solusd)
            if not r:
                continue
            w, side, sol, tok = r
            e = early[w].setdefault(c["mint"], {"sym": c["sym"], "buy_sol": 0.0, "sell_sol": 0.0, "first": tx.get("blockTime")})
            e["buy_sol" if side == "buy" else "sell_sol"] += sol
            buys += side == "buy"
        print(f"{c['sym']}: {len(sigs)} sigs, launch {(now - start) / 3600:.0f} h ago, early window {(end - start) / 60:.0f} min, "
              f"parsed {len(window)}, {buys} early buys by {sum(1 for w in early if c['mint'] in early[w])} wallets ({time.time() - t0:.0f}s)")
    multi = {w: m for w, m in early.items() if sum(1 for e in m.values() if e["buy_sol"] > 0) >= 2}
    big = sorted(((w, m) for w, m in early.items() if w not in multi),
                 key=lambda x: -sum(e["sell_sol"] - e["buy_sol"] for e in x[1].values()))
    big = [(w, m) for w, m in big if sum(e["sell_sol"] - e["buy_sol"] for e in m.values()) > 0.5][:max(0, 40 - len(multi))]
    out = []
    for w, m in list(multi.items()) + big:
        out.append({"wallet": w, "why": ("early on " if w in multi else "early profit on ") + ",".join(e["sym"] for e in m.values()),
                    "early": list(m.values())})
    print(f"\n{len(early)} early buyers; {len(multi)} were early on 2+ coins; {len(big)} single-coin early winners added")
    for o in out:
        print(f"  {o['wallet']} {o['why']}")
    print("=== EARLY JSON")
    print(json.dumps(out))
    print("=== END")


if __name__ == "__main__":
    main()
