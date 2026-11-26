"""Probe which public data sources a runner can reach for the copy-trade study.

Read-only. Prints the HTTP status and a short sample for each endpoint.
"""
import json
import urllib.request
import urllib.error

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
      "Accept": "application/json"}
WSOL = "So11111111111111111111111111111111111111112"
BONK = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"

def get(url, data=None, headers=None):
    h = dict(UA)
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:300]
    except Exception as e:  # noqa: BLE001
        return 0, str(e).encode()

def show(name, url, **kw):
    code, body = get(url, **kw)
    print(f"== {name}: HTTP {code} len {len(body)}")
    print("   " + body[:400].decode("utf-8", "replace").replace("\n", " "))
    return code, body

c, b = show("gt trending", "https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page=1")
pool = None
try:
    d = json.loads(b)["data"]
    for p in d[:15]:
        a = p["attributes"]
        print(f"   {a['name'][:30]:30} {p['id']} vol24 {a['volume_usd']['h24']} fdv {a.get('fdv_usd')} created {a.get('pool_created_at')}")
    pool = d[0]["attributes"]["address"]
    tok = d[0]["relationships"]["base_token"]["data"]["id"].split("_", 1)[1]
except Exception as e:  # noqa: BLE001
    print("   parse fail", e)
    tok = BONK
if pool:
    show("gt trades", f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool}/trades")
    show("gt ohlcv", f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool}/ohlcv/minute?aggregate=1&limit=5")
show("gmgn rank", "https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?orderby=swaps&direction=desc&limit=5")
show("gmgn top traders", f"https://gmgn.ai/defi/quotation/v1/tokens/top_traders/sol/{tok}?orderby=profit&direction=desc")
show("gmgn wallet", "https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/8HcYptCBAaPFWkmupiSAmysZ6Z8jB7N1c4YhVjhX7zbg?period=7d")
show("dexscreener boosts", "https://api.dexscreener.com/token-boosts/top/v1")
show("dexscreener token", f"https://api.dexscreener.com/latest/dex/tokens/{tok}")
show("solanatracker trending", "https://data.solanatracker.io/tokens/trending")
show("birdeye top traders", f"https://public-api.birdeye.so/defi/v2/tokens/top_traders?address={tok}", headers={"x-chain": "solana"})
show("jup price", f"https://lite-api.jup.ag/price/v3?ids={tok}")
body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [tok, {"limit": 3}]}).encode()
show("public rpc", "https://api.mainnet-beta.solana.com", data=body, headers={"Content-Type": "application/json"})
show("pump frontend coin", f"https://frontend-api-v3.pump.fun/coins/{tok}")
show("pumpportal", "https://pumpportal.fun/api/data/top-runners")
