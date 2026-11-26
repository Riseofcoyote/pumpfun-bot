"""Copy-trade study, wider search part 1: score wallets on about 30 days of trades.

Read-only. Reads a wallet list from tools/data/<name>.json, pages each wallet's
signatures back 30 days (cap MAX_TX), parses every transaction into buys and sells
(same parser as copy_wallets.py), scores the wallet with the same flags (bot, sniper,
insider, one_hit, few_coins) and adds a weekly check, so one lucky coin or one lucky
week can't carry a wallet. Wallets that pass get the same paper copy test: instant
(their fills plus our slippage and fixed cost) and next-minute (the next 1-minute
candle open after their buy and their first sell, plus full fees and slippage).
Prints one SCORE json line per wallet as it finishes.
"""
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import copy_wallets as cw  # noqa: E402

DAYS = 30
MAX_TX = int(os.environ.get("MAX_TX", "5000"))
DEADLINE_S = 105 * 60


def history(wallet, solusd):
    cutoff = time.time() - DAYS * 86400
    sigs, before = [], None
    while len(sigs) < MAX_TX:
        p = {"limit": 1000}
        if before:
            p["before"] = before
        page = cw.rpc("getSignaturesForAddress", [wallet, p]) or []
        if not page:
            break
        before = page[-1]["signature"]
        sigs += [s for s in page if not s.get("err") and (s.get("blockTime") or 0) >= cutoff]
        if len(page) < 1000 or (page[-1].get("blockTime") or 0) < cutoff:
            break
    sigs = sigs[:MAX_TX]
    span = (sigs[0].get("blockTime", 0) - sigs[-1].get("blockTime", 0)) if sigs else 0
    if len(sigs) >= 200 and span < len(sigs) * 60:
        return None, len(sigs), span
    events = []
    for s in sigs:
        tx = cw.rpc("getTransaction", [s["signature"], {"maxSupportedTransactionVersion": 0, "encoding": "json"}])
        if not tx:
            continue
        r = cw.parse_tx(tx, wallet, solusd)
        if not r:
            continue
        sol, tok = r
        t = tx.get("blockTime") or s.get("blockTime") or 0
        if len(tok) == 1:
            (m, d), = tok.items()
            if d > 0 and sol < -0.001:
                events.append({"t": t, "mint": m, "side": "buy", "sol": -sol, "tok": d})
            elif d < 0 and sol > 0.0005:
                events.append({"t": t, "mint": m, "side": "sell", "sol": sol, "tok": -d})
            elif d > 0:
                events.append({"t": t, "mint": m, "side": "in", "sol": 0, "tok": d})
            else:
                events.append({"t": t, "mint": m, "side": "out", "sol": 0, "tok": -d})
    return sorted(events, key=lambda e: e["t"]), len(sigs), span


def weekly(coins):
    now = time.time()
    weeks = {}
    for c in coins:
        k = int((now - c["first"]) // (7 * 86400))
        weeks[k] = weeks.get(k, 0) + c["pnl"]
    return {f"w{k}": round(v, 2) for k, v in sorted(weeks.items())}


def run(name):
    start = time.time()
    here = os.path.dirname(os.path.abspath(__file__))
    wallets = json.load(open(os.path.join(here, "data", f"{name}.json")))
    solusd = cw.sol_usd()
    print(f"{name}: {len(wallets)} wallets, {DAYS} days, up to {MAX_TX} tx each, SOL ${solusd:.2f}")
    for j, w in enumerate(wallets):
        if time.time() - start > DEADLINE_S:
            print("deadline: skipped " + " ".join(x["wallet"] for x in wallets[j:]))
            break
        t0 = time.time()
        ev, ntx, span = history(w["wallet"], solusd)
        print(f"  [{j + 1}/{len(wallets)}] {w['wallet']} {ntx} tx over {span / 86400:.1f} d, "
              f"{'bot-rate' if ev is None else str(len(ev)) + ' swaps/transfers'} ({time.time() - t0:.0f}s)")
        if ev is None:
            print("SCORE " + json.dumps({"wallet": w["wallet"], "why": w.get("why"), "tx": ntx, "span_h": round(span / 3600, 1),
                                         "flags": ["bot"], "coins": 0, "pnl_sol": 0, "win_rate": 0}))
            continue
        prices = cw.jup_prices({e["mint"] for e in ev})
        s, coins = cw.score(w["wallet"], ev, ntx, span, prices, solusd)
        s["why"] = w.get("why")
        s["weeks"] = weekly(coins)
        if s["coins"] < 10 and "few_coins" not in s["flags"]:
            s["flags"].append("few_coins")
        wk = list(s["weeks"].values())
        s["weeks_up"] = f"{sum(v > 0 for v in wk)}/{len(wk)}"
        s["capped"] = ntx >= MAX_TX
        ok = (not s["flags"] and s["win_rate"] >= 0.45 and s["pnl_sol"] > 0 and wk and sum(v > 0 for v in wk) * 2 >= len(wk))
        sim = None
        if ok:
            pairs = cw.best_pairs({c["mint"] for c in coins})
            sim = cw.copy_sim(sorted(coins, key=lambda c: c["first"])[-25:], pairs)
            s["copy_instant"], s["copy_delayed"] = sim
        hold = "-" if s["med_hold_s"] is None else f"{s['med_hold_s']:.0f}s"
        print(f"  {s['wallet']} coins {s['coins']} win {s['win_rate']:.0%} pnl {s['pnl_sol']:+.2f} SOL buy {s['med_buy_sol']:.2f} "
              f"hold {hold} coins/h {s['coins_per_h']} weeks {s['weeks']} flags {','.join(s['flags']) or '-'} | {s['why']}")
        if sim:
            print(f"     copy instant  {cw.fmt(sim[0])}")
            print(f"     copy delayed  {cw.fmt(sim[1])}")
        print("SCORE " + json.dumps(s))
    print("=== DONE")
