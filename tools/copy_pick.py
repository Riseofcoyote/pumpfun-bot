"""Copy-trade study, step 1: pick wallets from the top trending Solana meme coins.

Read-only. Takes the top 15 SOL-quoted meme pools on GeckoTerminal's trending
list, pulls each pool's recent trades (three size bands, so big traders show up
even on busy coins), and ranks the wallets by USD traded and by net SOL taken out
inside the window. Wallets that traded several trending coins, plus the top few
on each coin, become candidates for step 2 (copy_wallets.py), which reads each
wallet's own history on chain. Prints the candidates as JSON between markers.
"""
import json
import os
import time
import urllib.error
import urllib.request
from collections import defaultdict

GT = "https://api.geckoterminal.com/api/v2/networks/solana"
WSOL = "So11111111111111111111111111111111111111112"
NOT_MEME = {"PUMP", "BP", "USDC", "USDT", "JUP", "JTO", "RAY", "BONK", "WIF", "TRUMP", "ORCA", "PYTH"}
TOP_COINS = 15
BANDS = (None, 300, 2000)  # trade_volume_in_usd_greater_than
PER_COIN_TOP = 6
MAX_WALLETS = 90
SOURCE = os.environ.get("PICK_SOURCE", "trending")  # or "volume": top 24h volume pools


def get(url):
    for attempt in range(6):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "curl/8.5.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                out = json.loads(r.read())
            time.sleep(2.2)  # GeckoTerminal free tier: 30 calls a minute
            return out
        except urllib.error.HTTPError as e:
            print(f"  {e.code} on {url[:90]} retry {attempt + 1}")
            time.sleep(15 * (attempt + 1))
        except Exception as e:  # noqa: BLE001
            print(f"  {type(e).__name__} on {url[:90]} retry {attempt + 1}")
            time.sleep(10)
    return None


coins = []
for page in ((1, 2) if SOURCE == "trending" else (1, 2, 3, 4, 5)):
    if SOURCE == "volume":
        d = get(f"{GT}/pools?page={page}&sort=h24_volume_usd_desc&include=base_token")
    else:
        d = get(f"{GT}/trending_pools?page={page}&include=base_token")
    if not d:
        continue
    for p in d["data"]:
        a = p["attributes"]
        quote = p["relationships"]["quote_token"]["data"]["id"].split("_", 1)[1]
        base = p["relationships"]["base_token"]["data"]["id"].split("_", 1)[1]
        sym = a["name"].split(" / ")[0].strip()
        fdv = float(a.get("fdv_usd") or 0)
        if quote != WSOL or sym.upper() in NOT_MEME or fdv > 500e6:
            continue
        if any(c["mint"] == base for c in coins):
            continue
        coins.append({
            "sym": sym, "mint": base, "pool": a["address"], "fdv": fdv,
            "vol24": float(a["volume_usd"]["h24"] or 0), "created": a.get("pool_created_at"),
            "dex": p["relationships"].get("dex", {}).get("data", {}).get("id"),
        })
    if len(coins) >= TOP_COINS:
        break
coins = coins[:TOP_COINS]
print(f"{len(coins)} {SOURCE} meme coins (GeckoTerminal, {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}):")
for c in coins:
    print(f"  {c['sym'][:14]:14} {c['mint']} fdv ${c['fdv']:,.0f} vol24 ${c['vol24']:,.0f} pool since {c['created']} on {c['dex']}")

# per wallet per coin: usd bought, usd sold, sol in/out, trade count
w = defaultdict(lambda: defaultdict(lambda: {"buy_usd": 0.0, "sell_usd": 0.0, "n": 0, "first": None, "last": None}))
seen = set()
for c in coins:
    got = 0
    span = []
    for band in BANDS:
        q = f"{GT}/pools/{c['pool']}/trades"
        if band:
            q += f"?trade_volume_in_usd_greater_than={band}"
        d = get(q)
        if not d:
            continue
        for t in d["data"]:
            a = t["attributes"]
            key = (a["tx_hash"], a["tx_from_address"], a["kind"])
            if key in seen:
                continue
            seen.add(key)
            got += 1
            rec = w[a["tx_from_address"]][c["mint"]]
            usd = float(a.get("volume_in_usd") or 0)
            rec["buy_usd" if a["kind"] == "buy" else "sell_usd"] += usd
            rec["n"] += 1
            ts = a["block_timestamp"]
            span.append(ts)
            rec["first"] = min(rec["first"] or ts, ts)
            rec["last"] = max(rec["last"] or ts, ts)
    span.sort()
    c["trades_seen"] = got
    c["window"] = f"{span[0]} .. {span[-1]}" if span else ""
    print(f"  {c['sym'][:14]:14} {got} trades seen, window {c['window']}")

print(f"{len(w)} distinct wallets across the {len(coins)} coins")
rows = []
for wallet, per in w.items():
    vol = sum(r["buy_usd"] + r["sell_usd"] for r in per.values())
    net = sum(r["sell_usd"] - r["buy_usd"] for r in per.values())
    rows.append({"wallet": wallet, "coins": len(per), "vol_usd": round(vol), "net_usd": round(net),
                 "n": sum(r["n"] for r in per.values()),
                 "per": {m: {k: (round(v, 1) if isinstance(v, float) else v) for k, v in r.items()} for m, r in per.items()}})

multi = sorted([r for r in rows if r["coins"] >= 2], key=lambda r: (-r["coins"], -r["vol_usd"]))
print(f"wallets on 2+ trending coins: {len(multi)}; on 3+: {sum(r['coins'] >= 3 for r in rows)}; on 5+: {sum(r['coins'] >= 5 for r in rows)}")
for r in multi[:40]:
    print(f"  {r['wallet']} coins {r['coins']:2} trades {r['n']:4} vol ${r['vol_usd']:>9,} net in window ${r['net_usd']:>9,}")

pick = []
def add(r, why):
    if any(p["wallet"] == r["wallet"] for p in pick):
        return
    pick.append({"wallet": r["wallet"], "why": why, "coins": r["coins"], "vol_usd": r["vol_usd"], "net_usd": r["net_usd"], "n": r["n"]})

for c in coins:
    on = [r for r in rows if c["mint"] in r["per"]]
    by_net = sorted(on, key=lambda r: -(r["per"][c["mint"]]["sell_usd"] - r["per"][c["mint"]]["buy_usd"]))
    by_vol = sorted(on, key=lambda r: -(r["per"][c["mint"]]["sell_usd"] + r["per"][c["mint"]]["buy_usd"]))
    for r in by_net[:PER_COIN_TOP // 2]:
        add(r, f"top net {c['sym']}")
    for r in by_vol[:PER_COIN_TOP // 2]:
        add(r, f"top volume {c['sym']}")
for r in multi:
    if len(pick) >= MAX_WALLETS:
        break
    add(r, f"{r['coins']} trending coins")
pick = pick[:MAX_WALLETS]
print(f"{len(pick)} candidate wallets")
print("=== CANDIDATES JSON")
print(json.dumps({"at": time.time(), "coins": coins, "wallets": pick}))
print("=== END")
