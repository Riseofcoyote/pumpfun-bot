"""Read-only check of farmlive's live wallet: SOL balance, every token it holds,
and the status of each buy/sell signature in farmlive's trades. Public RPC only.

Usage: python tools/farmlive_check.py [signature ...]
"""

import json
import sys
import urllib.request

RPC = "https://api.mainnet-beta.solana.com"
WALLET = "Et8w4DyfUkqMebyMj5iv7ugTKfDKjzTDJ2RPDVu8zsRp"
TOKEN_PROGRAMS = (
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
)
DEFAULT_SIGS = [
    "GRnfzpvBLfM4NYoQ5pkUrUPkKoFyoaQSeHgugyarTto5CGVSi6qLZppqrdpc8URFWWtaVqD81L8D6oEGsVrnMxd",
]


def rpc(method: str, params: list) -> dict:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    req = urllib.request.Request(
        RPC, body.encode(), {"content-type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def main() -> None:
    bal = rpc("getBalance", [WALLET])["result"]["value"]
    print(f"wallet {WALLET} SOL {bal / 1e9:.4f}")
    for prog in TOKEN_PROGRAMS:
        doc = rpc(
            "getTokenAccountsByOwner",
            [WALLET, {"programId": prog}, {"encoding": "jsonParsed"}],
        )
        for acc in doc["result"]["value"]:
            info = acc["account"]["data"]["parsed"]["info"]
            amt = info["tokenAmount"]
            if int(amt["amount"]) > 0:
                print(f"  holds {amt['uiAmountString']} of {info['mint']}")
    sigs = sys.argv[1:] or DEFAULT_SIGS
    doc = rpc("getSignatureStatuses", [sigs, {"searchTransactionHistory": True}])
    for sig, st in zip(sigs, doc["result"]["value"]):
        print(f"sig {sig[:16]}… {json.dumps(st)}")


if __name__ == "__main__":
    main()
