"""Copy-trade study, step 2: read each candidate wallet's own trades on chain,
score it across every coin it traded, and paper-copy the promising ones.

Read-only: public RPC, Jupiter's price API, DexScreener and GeckoTerminal. No
wallet is loaded and nothing is signed. Candidates come from
tools/data/copy_candidates.json (written from copy_pick.py's output). Run as
copy_w0.py / copy_w1.py / copy_w2.py, which each take a third of the wallets so
three runners share the RPC load.

Per wallet: the last TX_PER_WALLET successful transactions are parsed from the
wallet's own SOL, WSOL and token balance changes, so any DEX counts. A coin
counts only if its first buy is inside the window. PnL marks unsold tokens at
Jupiter's current price.

Flags (any one drops the wallet from copy candidates):
- bot: over 60 swaps an hour, or a median hold under 30 s
- sniper: first buy within 60 s of the coin's creation on 30%+ of its coins
- insider: received a coin's tokens by plain transfer on 20%+ of its coins
- one_hit: over 70% of its total profit from one coin

Paper copy, 0.2 SOL a trade, costs per side = DEX fee (1.25% pump.fun and
PumpSwap, 0.25% Raydium, 1% other) + 0.0008 SOL priority/tip + 1% slippage:
- instant: our fills = their own fill prices (the best a copier could get)
- delayed: buy at the open of the next one-minute candle after their buy, sell
  at the next candle open after their first sell (about 0-60 s late, 30 s on
  average); unsold coins are marked at the last candle
"""
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

RPC = os.environ.get("RPC_URL", "https://solana-rpc.publicnode.com")  # ~15 tx/s free; mainnet-beta is ~0.3 tx/s
RPC_FALLBACK = "https://api.mainnet-beta.solana.com"
GT = "https://api.geckoterminal.com/api/v2/networks/solana"
WSOL = "So11111111111111111111111111111111111111112"
USD_MINTS = {"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
TX_PER_WALLET = int(os.environ.get("TX_PER_WALLET", "150"))
DEADLINE_S = 100 * 60  # stop fetching before the 120-minute job limit
STAKE = 0.2
FIXED_SOL = 0.0008
SLIP = 0.01
DEX_FEE = {"pumpfun": 0.0125, "pumpswap": 0.0125, "raydium": 0.0025}
RPC_GAP = float(os.environ.get("RPC_GAP", "0.06"))


def http(url, body=None, gap=0.0, tries=7):
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, data=body, headers={
                "User-Agent": "curl/8.5.0", "Accept": "application/json", "Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=40) as r:
                out = json.loads(r.read())
            if gap:
                time.sleep(gap)
            return out
        except urllib.error.HTTPError as e:
            wait = 3 * (attempt + 1) if e.code == 429 else 5
            if e.code in (400, 404):
                return None
            time.sleep(wait)
        except Exception:  # noqa: BLE001
            time.sleep(4)
    return None


_last_mb = [0.0]


def _mb(body):
    """mainnet-beta: full history, about 1 call a second sustained (0.6 s gap, no 429s)."""
    wait = 0.6 - (time.time() - _last_mb[0])
    if wait > 0:
        time.sleep(wait)
    out = http(RPC_FALLBACK, body, 0.0, tries=6)
    _last_mb[0] = time.time()
    return out


def rpc(method, params):
    """Signatures come from mainnet-beta (PublicNode only keeps about 1.3 days).
    Transactions try PublicNode first (fast, recent only), then mainnet-beta."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    if method == "getTransaction":
        out = http(RPC, body, RPC_GAP, tries=3)
        if out and out.get("result"):
            return out["result"]
    out = _mb(body)
    return (out or {}).get("result")


def sol_usd():
    d = http(f"https://lite-api.jup.ag/price/v3?ids={WSOL}")
    return float(d[WSOL]["usdPrice"])


def jup_prices(mints):
    out = {}
    mints = list(mints)
    for i in range(0, len(mints), 50):
        d = http("https://lite-api.jup.ag/price/v3?ids=" + ",".join(mints[i:i + 50]), gap=0.3) or {}
        for m, v in d.items():
            if v:
                out[m] = v
    return out


def parse_tx(tx, wallet, solusd):
    """Return (sol_delta, {mint: token_delta}) for the wallet, quote in SOL."""
    meta = tx.get("meta") or {}
    if meta.get("err"):
        return None
    keys = tx["transaction"]["message"]["accountKeys"]
    keys = [k if isinstance(k, str) else k.get("pubkey") for k in keys]
    if wallet not in keys:
        return None
    i = keys.index(wallet)
    sol = (meta["postBalances"][i] - meta["preBalances"][i]) / 1e9
    tok = defaultdict(float)
    for side, sign in (("preTokenBalances", -1), ("postTokenBalances", 1)):
        for b in meta.get(side) or []:
            if b.get("owner") != wallet:
                continue
            amt = float(b["uiTokenAmount"].get("uiAmountString") or 0)
            tok[b["mint"]] += sign * amt
    if WSOL in tok:
        sol += tok.pop(WSOL)
    for m in list(tok):
        if m in USD_MINTS:
            sol += tok.pop(m) / solusd
    tok = {m: d for m, d in tok.items() if abs(d) > 1e-9}
    return sol, tok


def wallet_history(wallet, solusd):
    sigs = []
    before = None
    while len(sigs) < TX_PER_WALLET:
        p = {"limit": min(1000, TX_PER_WALLET + 50)}
        if before:
            p["before"] = before
        page = rpc("getSignaturesForAddress", [wallet, p]) or []
        if not page:
            break
        sigs += [s for s in page if not s.get("err")]
        before = page[-1]["signature"]
        if len(page) < p["limit"]:
            break
    sigs = sigs[:TX_PER_WALLET]
    span = (sigs[0].get("blockTime", 0) - sigs[-1].get("blockTime", 0)) if sigs else 0
    if len(sigs) >= TX_PER_WALLET and span < len(sigs) * 60:
        return None, len(sigs), span  # over 60 tx an hour: a bot, no need to parse
    events = []
    for s in sigs:
        tx = rpc("getTransaction", [s["signature"], {"maxSupportedTransactionVersion": 0, "encoding": "json"}])
        if not tx:
            continue
        r = parse_tx(tx, wallet, solusd)
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


def positions(events):
    pos = {}
    for e in events:
        p = pos.setdefault(e["mint"], {"buys": [], "sells": [], "ins": 0, "complete": True})
        if e["side"] == "buy":
            p["buys"].append(e)
        elif e["side"] == "sell":
            if not p["buys"]:
                p["complete"] = False
            p["sells"].append(e)
        elif e["side"] == "in":
            p["ins"] += 1
            if not p["buys"]:
                p["complete"] = False
        elif e["side"] == "out" and not p["buys"]:
            p["complete"] = False
    return pos


def score(wallet, events, ntx, span, prices, solusd):
    pos = positions(events)
    coins = []
    ins_coins = 0
    for m, p in pos.items():
        if p["ins"]:
            ins_coins += 1
        if not p["complete"] or not p["buys"]:
            continue
        spent = sum(b["sol"] for b in p["buys"])
        got = sum(s["sol"] for s in p["sells"])
        held = sum(b["tok"] for b in p["buys"]) - sum(s["tok"] for s in p["sells"])
        px = prices.get(m, {}).get("usdPrice")
        mark = max(held, 0) * float(px) / solusd if px else 0.0
        pnl = got + mark - spent
        first = p["buys"][0]["t"]
        hold = (p["sells"][0]["t"] - first) if p["sells"] else None
        created = prices.get(m, {}).get("createdAt")
        snipe = None
        if created:
            ct = datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp()
            snipe = (first - ct) <= 60
        coins.append({"mint": m, "spent": spent, "pnl": pnl, "pct": pnl / spent if spent else 0, "first": first,
                      "first_sell": p["sells"][0]["t"] if p["sells"] else None, "hold": hold, "snipe": snipe,
                      "buy_px": p["buys"][0]["sol"] / p["buys"][0]["tok"],
                      "sell_px": (p["sells"][0]["sol"] / p["sells"][0]["tok"]) if p["sells"] else None,
                      "open": held > 1e-6 and not p["sells"], "unpriced": held > 1e-6 and not px})
    swaps = sum(1 for e in events if e["side"] in ("buy", "sell"))
    hrs = max(span / 3600, 0.25)
    n = len(coins)
    wins = sum(c["pnl"] > 0 for c in coins)
    tot = sum(c["pnl"] for c in coins)
    pos_pnl = sorted((c["pnl"] for c in coins if c["pnl"] > 0), reverse=True)
    holds = [c["hold"] for c in coins if c["hold"] is not None]
    snipes = [c["snipe"] for c in coins if c["snipe"] is not None]
    s = {
        "wallet": wallet, "tx": ntx, "span_h": round(span / 3600, 1), "swaps": swaps, "swaps_per_h": round(swaps / hrs, 1),
        "coins": n, "wins": wins, "win_rate": wins / n if n else 0, "pnl_sol": tot,
        "med_pct": statistics.median([c["pct"] for c in coins]) if coins else 0,
        "med_buy_sol": statistics.median([c["spent"] for c in coins]) if coins else 0,
        "med_hold_s": statistics.median(holds) if holds else None,
        "snipe_rate": (sum(snipes) / len(snipes)) if snipes else 0,
        "insider_rate": ins_coins / max(len(pos), 1),
        "top_share": (pos_pnl[0] / sum(pos_pnl)) if pos_pnl and len(pos_pnl) > 1 else (1.0 if pos_pnl else 0),
        "coins_per_h": round(n / hrs, 2),
    }
    flags = []
    if s["swaps_per_h"] > 60 or (s["med_hold_s"] is not None and s["med_hold_s"] < 30):
        flags.append("bot")
    if s["snipe_rate"] >= 0.3:
        flags.append("sniper")
    if s["insider_rate"] >= 0.2:
        flags.append("insider")
    if tot > 0 and s["top_share"] > 0.7:
        flags.append("one_hit")
    if n < 5:
        flags.append("few_coins")
    s["flags"] = flags
    return s, coins


def cost(dex):
    return DEX_FEE.get(dex, 0.01) + FIXED_SOL / STAKE + SLIP


def best_pairs(mints):
    out = {}
    mints = list(mints)
    for i in range(0, len(mints), 30):
        d = http("https://api.dexscreener.com/tokens/v1/solana/" + ",".join(mints[i:i + 30]), gap=1.0) or []
        for p in d:
            m = p["baseToken"]["address"]
            liq = (p.get("liquidity") or {}).get("usd") or 0
            if m not in out or liq > out[m][2]:
                out[m] = (p["pairAddress"], p.get("dexId"), liq)
    return out


def candles(pool, t_from, t_to):
    rows = []
    before = int(t_to) + 3600
    while before > t_from:
        d = http(f"{GT}/pools/{pool}/ohlcv/minute?aggregate=1&limit=1000&before_timestamp={before}", gap=2.2)
        lst = (((d or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
        if not lst:
            break
        rows += lst
        before = min(r[0] for r in lst)
        if len(lst) < 1000:
            break
    rows = sorted({r[0]: r for r in rows}.values())
    return rows  # [ts, o, h, l, c, v]


def next_open(rows, t):
    for r in rows:
        if r[0] > t:
            return r[1]
    return None


def copy_sim(coins, pairs):
    inst, dly = [], []
    for c in coins:
        pair = pairs.get(c["mint"])
        dex = pair[1] if pair else None
        k = cost(dex)
        if c["sell_px"]:
            inst.append(c["sell_px"] / c["buy_px"] - 1 - 2 * (FIXED_SOL / STAKE + SLIP))  # their fills already include the DEX fee
        if not pair:
            continue
        rows = candles(pair[0], c["first"] - 120, (c["first_sell"] or time.time()) + 180)
        if not rows:
            continue
        b = next_open(rows, c["first"])
        if c["first_sell"]:
            s = next_open(rows, c["first_sell"]) or rows[-1][4]
        else:
            s = rows[-1][4]
        if b and s:
            dly.append(s / b - 1 - 2 * k)
    return inst, dly


def fmt(xs):
    if not xs:
        return "n 0"
    w = sum(x > 0 for x in xs) / len(xs)
    return f"n {len(xs):3} win {w:4.0%} mean {statistics.mean(xs):+7.1%} med {statistics.median(xs):+7.1%}"


def report(s, sim=None):
    hold = "-" if s["med_hold_s"] is None else f"{s['med_hold_s']:.0f}s"
    line = (f"  {s['wallet']} coins {s['coins']:3} win {s['win_rate']:4.0%} pnl {s['pnl_sol']:+8.2f} SOL med {s['med_pct']:+6.0%} "
            f"buy {s['med_buy_sol']:.2f} hold {hold:>6} swaps/h {s['swaps_per_h']:5} span {s['span_h']}h "
            f"snipe {s['snipe_rate']:.0%} xfer {s['insider_rate']:.0%} top {s['top_share']:.0%} "
            f"flags {','.join(s['flags']) or '-'} | {s['why']} | trending {','.join(s['trending_hit'])}")
    print(line)
    if sim:
        print(f"     copy instant  {fmt(sim[0])}")
        print(f"     copy delayed  {fmt(sim[1])}")
    print("SCORE " + json.dumps(s))


def main(shard, nshards):
    start = time.time()
    here = os.path.dirname(os.path.abspath(__file__))
    cand = json.load(open(os.path.join(here, "data", "copy_candidates.json")))
    wallets = [w for i, w in enumerate(cand["wallets"]) if i % nshards == shard]
    trending = {c["mint"]: c["sym"] for c in cand["coins"]}
    solusd = sol_usd()
    print(f"shard {shard}/{nshards}: {len(wallets)} wallets, SOL ${solusd:.2f}, {TX_PER_WALLET} tx each")
    for j, w in enumerate(wallets):
        if time.time() - start > DEADLINE_S:
            print(f"deadline: skipped {len(wallets) - j} wallets: " + " ".join(x["wallet"] for x in wallets[j:]))
            break
        t0 = time.time()
        ev, ntx, span = wallet_history(w["wallet"], solusd)
        print(f"  [{j + 1}/{len(wallets)}] {w['wallet']} {ntx} tx over {span / 3600:.1f} h, "
              f"{'bot-rate, skipped' if ev is None else str(len(ev)) + ' swaps/transfers'} ({time.time() - t0:.0f}s)")
        if ev is None:
            s = {"wallet": w["wallet"], "tx": ntx, "span_h": round(span / 3600, 1), "swaps": None,
                 "swaps_per_h": round(ntx / max(span / 3600, 0.01), 1), "coins": 0, "wins": 0, "win_rate": 0,
                 "pnl_sol": 0, "med_pct": 0, "med_buy_sol": 0, "med_hold_s": None, "snipe_rate": 0,
                 "insider_rate": 0, "top_share": 0, "coins_per_h": 0, "flags": ["bot"], "why": w["why"], "trending_hit": []}
            report(s)
            continue
        prices = jup_prices({e["mint"] for e in ev})
        s, coins = score(w["wallet"], ev, ntx, span, prices, solusd)
        s["why"] = w["why"]
        s["trending_hit"] = sorted({trending[c["mint"]] for c in coins if c["mint"] in trending})
        sim = None
        if not s["flags"] and s["win_rate"] >= 0.4 and s["pnl_sol"] > 0:
            pairs = best_pairs({c["mint"] for c in coins})
            sim = copy_sim(sorted(coins, key=lambda c: c["first"])[-25:], pairs)
            s["copy_instant"], s["copy_delayed"] = sim
        report(s, sim)
    print("=== DONE")


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]))
