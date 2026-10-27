"""Verify a pasted private key with stray whitespace still loads, and a disabled
bot config does not demand its environment variables.

Bugs: a trailing space in SOLANA_PRIVATE_KEY crashed startup with
"Invalid character ' '"; disabled stock configs logged GEYSER_ENDPOINT errors on
every start. Offline; exits non-zero on failure.
"""

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

FAILS: list[str] = []


def check(cond: bool, label: str) -> None:
    print(("PASS " if cond else "FAIL ") + label)
    if not cond:
        FAILS.append(label)


from config_loader import load_bot_config  # noqa: E402

with tempfile.TemporaryDirectory() as d:
    p = Path(d) / "off.yaml"
    p.write_text('name: "off"\nenabled: false\ngeyser:\n  endpoint: "${NOT_SET_ANYWHERE_123}"\n')
    os.environ.pop("NOT_SET_ANYWHERE_123", None)
    try:
        cfg = load_bot_config(str(p))
        check(cfg["enabled"] is False, "disabled config loads without its env vars")
    except ValueError as e:
        check(False, f"disabled config loads without its env vars ({e})")

try:
    import base58
    from solders.keypair import Keypair

    from core.wallet import Wallet
except ImportError:
    print("SKIP wallet checks (solders not installed)")
else:
    kp = Keypair()
    key = base58.b58encode(bytes(kp)).decode()
    for label, raw in [("trailing space", key + " "), ("newline", "\n" + key + "\n"),
                       ("quotes", f'"{key}"')]:
        check(Wallet(raw).pubkey == kp.pubkey(), f"key with {label} loads")
    try:
        Wallet("abandon ability able about above absent absorb abstract absurd abuse access accident")
        check(False, "seed phrase rejected with a clear message")
    except ValueError as e:
        check("seed phrase" in str(e), "seed phrase rejected with a clear message")
    try:
        Wallet(key[:-5] + "0OIl!")
        check(False, "bad key rejected")
    except ValueError as e:
        check(key[:10] not in str(e), "bad-key error does not echo the key")

sys.exit(1 if FAILS else 0)
