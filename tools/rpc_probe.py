"""Read-only: times public Solana RPC endpoints for wallet-history reads."""
import json, time, urllib.request, urllib.error
W = "2yDEStr5muT5f11k8PUZTgbpMt77LQ8CVf4DEzSNA4ia"
EPS = ["https://api.mainnet-beta.solana.com", "https://solana-rpc.publicnode.com", "https://solana.drpc.org",
       "https://solana.api.onfinality.io/public", "https://api.blockeden.xyz/solana/public"]
def post(ep, body):
    req = urllib.request.Request(ep, data=json.dumps(body).encode(), headers={"User-Agent": "curl/8.5.0", "Content-Type": "application/json"})
    t = time.time()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read()), time.time() - t, 200
    except urllib.error.HTTPError as e:
        return None, time.time() - t, e.code
    except Exception as e:  # noqa: BLE001
        return None, time.time() - t, type(e).__name__
for ep in EPS:
    out, dt, code = post(ep, {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [W, {"limit": 1000}]})
    sigs = [s["signature"] for s in ((out or {}).get("result") or [])]
    print(f"{ep}: sigs {len(sigs)} code {code} {dt:.1f}s")
    if not sigs:
        continue
    ok = fail = 0
    t0 = time.time()
    for s in sigs[:30]:
        o, dt, code = post(ep, {"jsonrpc": "2.0", "id": 1, "method": "getTransaction", "params": [s, {"maxSupportedTransactionVersion": 0, "encoding": "json"}]})
        if o and o.get("result"): ok += 1
        else: fail += 1; print("   fail", code, (o or {}).get("error"))
    print(f"   30 single getTransaction: ok {ok} fail {fail} in {time.time() - t0:.1f}s")
    batch = [{"jsonrpc": "2.0", "id": i, "method": "getTransaction", "params": [s, {"maxSupportedTransactionVersion": 0, "encoding": "json"}]} for i, s in enumerate(sigs[30:50])]
    o, dt, code = post(ep, batch)
    n = sum(1 for x in (o or []) if isinstance(x, dict) and x.get("result")) if isinstance(o, list) else 0
    print(f"   batch of 20: code {code} ok {n} in {dt:.1f}s {'' if isinstance(o, list) else str(o)[:200]}")
    # oldest page depth check
    o, dt, code = post(ep, {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [W, {"limit": 1000, "before": sigs[-1]}]})
    print(f"   2nd sig page: {len(((o or {}).get('result') or []))} in {dt:.1f}s code {code}")
