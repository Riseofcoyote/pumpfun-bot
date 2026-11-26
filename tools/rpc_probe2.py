"""Read-only: how far back PublicNode reaches, and mainnet-beta's sustained getTransaction rate."""
import json, time, urllib.request, urllib.error
W = "2yDEStr5muT5f11k8PUZTgbpMt77LQ8CVf4DEzSNA4ia"
MB, PN = "https://api.mainnet-beta.solana.com", "https://solana-rpc.publicnode.com"
def post(ep, method, params):
    req = urllib.request.Request(ep, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
                                 headers={"User-Agent": "curl/8.5.0", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read()), 200
    except urllib.error.HTTPError as e:
        return None, e.code
    except Exception as e:  # noqa: BLE001
        return None, type(e).__name__
for ep in (PN, MB):
    sigs, before = [], None
    for _ in range(60):
        p = {"limit": 1000}
        if before: p["before"] = before
        o, c = post(ep, "getSignaturesForAddress", [W, p])
        page = (o or {}).get("result") or []
        if not page: break
        sigs += page; before = page[-1]["signature"]
        if len(page) < 1000: break
    print(f"{ep}: {len(sigs)} sigs back {(time.time() - sigs[-1]['blockTime']) / 86400:.1f} days")
    if ep == MB:
        old = [s for s in sigs if time.time() - s["blockTime"] > 10 * 86400][:5]
        for s in old:
            o, c = post(PN, "getTransaction", [s["signature"], {"maxSupportedTransactionVersion": 0}])
            print(f"   PN old tx ({(time.time() - s['blockTime']) / 86400:.0f} d): code {c} result {'yes' if (o or {}).get('result') else 'no'} {(o or {}).get('error')}")
        mid = [s for s in sigs if time.time() - s["blockTime"] > 5 * 86400]
        for gap in (1.1, 0.6):
            ok = bad = 0; t0 = time.time()
            for s in mid[:40]:
                o, c = post(MB, "getTransaction", [s["signature"], {"maxSupportedTransactionVersion": 0}])
                if (o or {}).get("result"): ok += 1
                else: bad += 1
                time.sleep(gap)
            print(f"   MB gap {gap}: ok {ok} 429/fail {bad} in {time.time() - t0:.0f}s")
            mid = mid[40:]
            time.sleep(10)
