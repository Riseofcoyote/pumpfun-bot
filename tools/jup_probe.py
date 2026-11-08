"""Print Jupiter and DexScreener views of one token's tradability. Read-only."""

import json
import sys
import urllib.request

MINT = sys.argv[1]
SOL = "So11111111111111111111111111111111111111112"


def get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "pumpfun-bot-probe/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:  # noqa: S310
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return {"_http": e.code, "_body": e.read().decode()[:300]}


ds = get(f"https://api.dexscreener.com/latest/dex/tokens/{MINT}")
for p in (ds.get("pairs") or [])[:6]:
    print("DS", p.get("dexId"), p.get("pairAddress"), "quote", p["quoteToken"]["symbol"],
          "liq", (p.get("liquidity") or {}).get("usd"), "px", p.get("priceUsd"), "labels", p.get("labels"))
    print("   volume", p.get("volume"), "txns", p.get("txns"), "change", p.get("priceChange"),
          "mcap", p.get("marketCap"), "created", p.get("pairCreatedAt"))
for lam in (10_000_000, 100_000_000):
    q = get(f"https://lite-api.jup.ag/swap/v1/quote?inputMint={SOL}&outputMint={MINT}&amount={lam}&slippageBps=300")
    print("JUP", lam, "out", q.get("outAmount"), "impact", q.get("priceImpactPct"),
          "route", [(r["swapInfo"].get("label"), r["swapInfo"].get("ammKey")) for r in q.get("routePlan", [])],
          q.get("_http"), q.get("_body") or q.get("error"))
    q = get(f"https://lite-api.jup.ag/swap/v1/quote?inputMint={SOL}&outputMint={MINT}&amount={lam}&slippageBps=300&restrictIntermediateTokens=true&onlyDirectRoutes=true")
    print("JUP direct", lam, "out", q.get("outAmount"), "impact", q.get("priceImpactPct"),
          "route", [(r["swapInfo"].get("label"), r["swapInfo"].get("ammKey")) for r in q.get("routePlan", [])], q.get("error"))
print("JUP price", get(f"https://lite-api.jup.ag/price/v3?ids={MINT}"))
print("JUP token", get(f"https://lite-api.jup.ag/tokens/v2/search?query={MINT}"))
