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


RECENT_S = 1.25 * 86400   # PublicNode keeps about this much, fast to read
SLOW_BUDGET = int(os.environ.get("SLOW_BUDGET", "2400"))  # older tx read from mainnet-beta at ~1/s
BLOCKS = 4


def to_event(tx, s, wallet, solusd):
    r = cw.parse_tx(tx, wallet, solusd)
    if not r:
        return None
    sol, tok = r
    t = tx.get("blockTime") or s.get("blockTime") or 0
    if len(tok) != 1:
        return None
    (m, d), = tok.items()
    if d > 0 and sol < -0.001:
        return {"t": t, "mint": m, "side": "buy", "sol": -sol, "tok": d}
    if d < 0 and sol > 0.0005:
        return {"t": t, "mint": m, "side": "sell", "sol": sol, "tok": -d}
    return {"t": t, "mint": m, "side": "in" if d > 0 else "out", "sol": 0, "tok": abs(d)}


def history(wallet, solusd):
    now = time.time()
    cutoff = now - DAYS * 86400
    sigs, before = [], None
    for _ in range(100):
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
    span = (sigs[0].get("blockTime", 0) - sigs[-1].get("blockTime", 0)) if sigs else 0
    info = {"tx30": len(sigs), "days": round(span / 86400, 1)}
    if len(sigs) >= 200 and span < len(sigs) * 60:
        return None, info
    recent = [s for s in sigs if s["blockTime"] >= now - RECENT_S]
    older = [s for s in sigs if s["blockTime"] < now - RECENT_S]
    if len(older) > SLOW_BUDGET:
        size = SLOW_BUDGET // BLOCKS
        step = len(older) // BLOCKS
        older = [x for b in range(BLOCKS) for x in older[b * step + (step - size) // 2: b * step + (step - size) // 2 + size]]
    picked = recent[:MAX_TX] + older
    info["parsed"] = len(picked)
    info["sampled"] = len(picked) < len(sigs)
    events = []
    for s in picked:
        tx = cw.rpc("getTransaction", [s["signature"], {"maxSupportedTransactionVersion": 0, "encoding": "json"}])
        if tx:
            e = to_event(tx, s, wallet, solusd)
            if e:
                events.append(e)
    if info["sampled"]:
        # a buy inside an older sampled block with no sell seen may have been sold in the gap: drop it
        sold = {e["mint"] for e in events if e["side"] == "sell"}
        old_open = {e["mint"] for e in events if e["side"] == "buy" and e["t"] < now - RECENT_S and e["mint"] not in sold}
        events = [e for e in events if e["mint"] not in old_open]
    return sorted(events, key=lambda e: e["t"]), info


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
        ev, info = history(w["wallet"], solusd)
        print(f"  [{j + 1}/{len(wallets)}] {w['wallet']} {info} "
              f"{'bot-rate' if ev is None else str(len(ev)) + ' swaps/transfers'} ({time.time() - t0:.0f}s)")
        if ev is None:
            print("SCORE " + json.dumps({"wallet": w["wallet"], "why": w.get("why"), **info,
                                         "flags": ["bot"], "coins": 0, "pnl_sol": 0, "win_rate": 0}))
            continue
        ntx, span = info["parsed"], info["days"] * 86400
        prices = cw.jup_prices({e["mint"] for e in ev})
        s, coins = cw.score(w["wallet"], ev, ntx, span, prices, solusd)
        s["why"] = w.get("why")
        s["weeks"] = weekly(coins)
        if s["coins"] < 10 and "few_coins" not in s["flags"]:
            s["flags"].append("few_coins")
        wk = list(s["weeks"].values())
        s["weeks_up"] = f"{sum(v > 0 for v in wk)}/{len(wk)}"
        s.update(info)
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
