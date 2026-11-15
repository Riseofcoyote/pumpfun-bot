"""Read-only check of the farm-relaunch mints the testers trade: token program,
authorities, Token-2022 extensions (a permanent delegate can take tokens out of
any holder's account), and what moved farmlive's tokens out of its wallet.
Public RPC only.

Usage: python tools/mint_check.py [mint ...]
"""

import json
import sys
import time
import urllib.request

RPC = "https://api.mainnet-beta.solana.com"
WALLET = "Et8w4DyfUkqMebyMj5iv7ugTKfDKjzTDJ2RPDVu8zsRp"
DEFAULT_MINTS = {
    "ANTH-Azb3 (farmlive buy 11-15 01:32, gone 01:36)": "Azb3qQ2JXvqrsqtBBLj1MNwunqzsPfRkHA8ymHbYdWPd",
    "ANTH-Ev9c (farmlive, gone)": "Ev9c1q8zDMU9LYGV3LjxbHenhtmyXnqAnXcdC9phHnAC",
    "AVATARS (farmlive, sold for 0)": "Fxb2YG9gywxsSJEgUwZMzTfpW3RLVbAkiY7ZdeqCMd7",
    "BILL (farmlive, sold for 0)": "LDDvrYcZjk6aJaJ6HPZuitvNdzYrayr46ebUoG4apDN",
    "ANTH-7ScQ (trending)": "7ScQZ9qoZpLvpYptMcpeuNnvKaCdN9UF6tYus1p83DK4",
    "ANTH-HPS5 (trending)": "HPS5sz5j6dz33DXtpzdVoph45bByugLRWwHqtTvKr9gT",
    "XTEN-DMvc (trending)": "DMvcPrWsThg3eyM96jEFTqFYrVSEV7jVzU5HEggRdTzP",
    "XTEN-9Pap (trending)": "9PapRL1cYoQ2bjAb6o3f2vbPz3euriyTE715Q3ycw5Ui",
    "PREDICT-8Cyk (trending)": "8CykGobYKN3og6VqeU17Dfhk5RZe6v7VJLQC8oUoC7yN",
    "OPNW (trending)": "7wDZG1tf4XV2ftnQvT4h9LPEnyREhgeFVNKLKvZZnpry",
}


def rpc(method: str, params: list) -> dict:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    req = urllib.request.Request(
        RPC, body.encode(), {"content-type": "application/json"}
    )
    for i in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception as e:  # noqa: BLE001
            print(f"  rpc {method} failed: {e}")
            time.sleep(3 * (i + 1))
    return {}


def check_mint(label: str, mint: str) -> None:
    doc = rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
    v = (doc.get("result") or {}).get("value")
    print(f"\n== {label} {mint}")
    if not v:
        print("  no account")
        return
    prog = v["owner"]
    info = v["data"]["parsed"]["info"] if isinstance(v["data"], dict) else {}
    print(f"  program {'token-2022' if prog.startswith('Tokenz') else prog}")
    print(f"  mintAuthority {info.get('mintAuthority')} freezeAuthority {info.get('freezeAuthority')}")
    for ext in info.get("extensions", []):
        print(f"  ext {ext.get('extension')}: {json.dumps(ext.get('state'))[:300]}")


def wallet_moves(mint: str) -> None:
    """Print transactions touching the wallet's token account for this mint."""
    doc = rpc("getSignaturesForAddress", [WALLET, {"limit": 60}])
    sigs = [s["signature"] for s in doc.get("result", [])]
    print(f"\n== wallet txs touching {mint[:8]}")
    for sig in sigs:
        tx = rpc(
            "getTransaction",
            [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
        ).get("result")
        time.sleep(0.4)
        if not tx:
            continue
        meta = tx["meta"]
        pre = {
            b["accountIndex"]: b
            for b in meta.get("preTokenBalances", [])
            if b["mint"] == mint
        }
        post = {
            b["accountIndex"]: b
            for b in meta.get("postTokenBalances", [])
            if b["mint"] == mint
        }
        if not pre and not post:
            continue
        keys = [k["pubkey"] for k in tx["transaction"]["message"]["accountKeys"]]
        signers = [
            k["pubkey"] for k in tx["transaction"]["message"]["accountKeys"] if k["signer"]
        ]
        t = time.strftime("%m-%d %H:%M:%S", time.gmtime(tx["blockTime"]))
        print(f"  {t} {sig[:16]} signers {[s[:8] for s in signers]} err {meta.get('err')}")
        for idx in sorted(set(pre) | set(post)):
            a = (pre.get(idx) or {}).get("uiTokenAmount", {}).get("uiAmount")
            b = (post.get(idx) or {}).get("uiTokenAmount", {}).get("uiAmount")
            owner = (post.get(idx) or pre.get(idx) or {}).get("owner", "")
            print(f"    acct {keys[idx][:8]} owner {owner[:8]} {a} -> {b}")
        progs = {
            ix.get("programId", "")[:8]
            for ix in tx["transaction"]["message"]["instructions"]
        }
        print(f"    programs {sorted(progs)}")
        for line in (meta.get("logMessages") or [])[:12]:
            print(f"    | {line[:140]}")


def main() -> None:
    mints = (
        {m[:8]: m for m in sys.argv[1:]} if len(sys.argv) > 1 else DEFAULT_MINTS
    )
    for label, mint in mints.items():
        check_mint(label, mint)
        time.sleep(0.5)
    wallet_moves(DEFAULT_MINTS["ANTH-Azb3 (farmlive buy 11-15 01:32, gone 01:36)"])


if __name__ == "__main__":
    main()
