"""Live farm-grind trader: buy once per matching coin, hold to 15 h or a 20% trail.

The only strategy that has paid steadily in paper: serial relaunches like
XTEN / PREDICT (``narrative.engine.farm_rule``). Paper ``farm_hold`` bought
XTEN at 8.5 h and sold at the 15 h cutoff for +38.6%; hourly re-entries on
the same coin won ~82% of the time at +4% median.

Real funds when FARMLIVE_ENABLED=1, otherwise a dry run that logs what it
would do. Swaps go through Jupiter's swap API (v0 transactions, signed here,
sent to the plan RPC). Env: FARMLIVE_DATA_DIR, FARMLIVE_ENABLED,
FARMLIVE_SIZE_SOL (default 0.1), FARMLIVE_MAX_POSITIONS (default 2),
FARMLIVE_SELFTEST_SOL (a one-time buy-and-sell of this size at start, then
off), SOLANA_PRIVATE_KEY, SOLANA_NODE_RPC_ENDPOINT. The key is never logged.
"""

from __future__ import annotations

import asyncio
import base64
import csv
import json
import logging
import logging.handlers
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import base58
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

from narrative.engine import FARM_SEED_SYMBOLS, ds_search_pools, farm_rule
from trending import feeds
from utils.logger import install_secret_redaction

DATA = Path(os.environ.get("FARMLIVE_DATA_DIR", "trades/farmlive"))
ENABLED = os.environ.get("FARMLIVE_ENABLED") == "1"
SIZE_SOL = float(os.environ.get("FARMLIVE_SIZE_SOL", "0.1"))
MAX_POSITIONS = int(os.environ.get("FARMLIVE_MAX_POSITIONS", "2"))
SELFTEST_SOL = float(os.environ.get("FARMLIVE_SELFTEST_SOL", "0") or 0)
RESERVE_SOL = 0.02  # never spend the wallet below this (fees, token-account rent)
EXIT_AGE_H = 15.0
TRAIL = 0.20
LIQ_DROP = 0.35  # liquidity pulled this far below entry is the usual lead-in to a rug
LIQ_EVERY = 120
POOL_FEE = 0.0125
EXEC_CHECK_EVERY = 120  # seconds between real Jupiter sell quotes on open positions
HELD_EVERY = 180
RELAUNCH_PROOF_H = 7 * 24  # a learned ticker needs 2+ mints within this window  # seconds between wallet checks for positions sold by hand
SEARCH_EVERY = 300
LEARN_EVERY = 900
WATCH_EVERY = 30
BUY_SLIPPAGE_BPS = 300
SELL_SLIPPAGE_BPS = 800
MAX_FILL_COST = 0.06  # fee + impact vs the pool's price; the pool fee alone is ~1.25%
SOL_MINT = "So11111111111111111111111111111111111111112"
JUP_SWAP = "https://lite-api.jup.ag/swap/v1"
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"

logger = logging.getLogger("farmlive")


def load_keypair() -> Keypair:
    key = os.environ.get("SOLANA_PRIVATE_KEY", "").strip().strip("'\"").strip()
    if not key or len(key.split()) > 1:
        raise SystemExit("SOLANA_PRIVATE_KEY missing or not a base58 private key")
    try:
        return Keypair.from_bytes(base58.b58decode(key))
    except ValueError:
        raise SystemExit("SOLANA_PRIVATE_KEY is not a valid base58 private key") from None


def jup(url: str, body: dict | None = None, timeout: float = 20) -> dict | None:
    """Jupiter call with up to 4 tries on 429 (the testers on this machine share its limit)."""
    doc = None
    for wait in (0, 2, 5, 10):
        time.sleep(wait)
        doc = http(url, body, timeout)
        if not (doc and doc.get("_http") == 429):
            return doc
    return doc


def http(url: str, body: dict | None = None, timeout: float = 20) -> dict | None:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Accept": "application/json", "User-Agent": "pumpfun-bot-farmlive/1.0"}
    if data:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return {"_http": e.code, "_body": e.read().decode()[:300]}
        except OSError:
            return {"_http": e.code}
    except (urllib.error.URLError, TimeoutError, ValueError, OSError) as e:
        return {"_error": type(e).__name__}


class Chain:
    """Plan RPC first (sends and confirms), public RPC as fallback for reads."""

    def __init__(self) -> None:
        plan = os.environ.get("SOLANA_NODE_RPC_ENDPOINT")
        self.urls = ([plan] if plan else []) + [PUBLIC_RPC]

    def call(self, method: str, params: list) -> dict | None:
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        last = None
        for url in self.urls:
            doc = http(url, body, timeout=15)
            if doc and "result" in doc:
                return doc
            last = doc
        return last

    def sol_balance(self, owner: str) -> int | None:
        doc = self.call("getBalance", [owner, {"commitment": "confirmed"}])
        return (doc or {}).get("result", {}).get("value") if doc and "result" in doc else None

    def token_balance(self, owner: str, mint: str) -> int | None:
        """Raw token units the owner holds of mint across its accounts; None if unknown."""
        doc = self.call("getTokenAccountsByOwner",
                        [owner, {"mint": mint}, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        if not doc or "result" not in doc:
            return None
        total = 0
        for acc in doc["result"].get("value") or []:
            info = acc["account"]["data"]["parsed"]["info"]
            total += int(info["tokenAmount"]["amount"])
        return total

    def send(self, tx_b64: str) -> str | None:
        doc = self.call("sendTransaction", [tx_b64, {"encoding": "base64", "skipPreflight": False,
                                                     "preflightCommitment": "confirmed", "maxRetries": 3}])
        if doc and "result" in doc:
            return doc["result"]
        logger.warning(f"[send] rejected: {str(doc)[:300]}")
        return None

    def status(self, sig: str) -> str:
        """SUCCESS, REVERTED or UNCONFIRMED."""
        doc = self.call("getSignatureStatuses", [[sig], {"searchTransactionHistory": True}])
        val = ((doc or {}).get("result") or {}).get("value") or [None]
        st = val[0]
        if not st:
            return "UNCONFIRMED"
        if st.get("err") is not None:
            return "REVERTED"
        if st.get("confirmationStatus") in ("confirmed", "finalized"):
            return "SUCCESS"
        return "UNCONFIRMED"


class FarmLive:
    def __init__(self, kp: Keypair | None) -> None:
        self.kp = kp
        self.owner = str(kp.pubkey()) if kp else ""
        self.chain = Chain()
        self.symbols: set[str] = set(FARM_SEED_SYMBOLS)
        self.positions: dict[str, dict] = {}
        self.bought: set[str] = set()
        self.selftest_done = False
        self.rate_limited = False

    # ----- state
    def load(self) -> None:
        p = DATA / "state.json"
        if p.exists():
            st = json.loads(p.read_text())
            self.positions = st.get("positions", {})
            self.bought = set(st.get("bought", []))
            self.symbols |= set(st.get("symbols", []))
            self.selftest_done = st.get("selftest_done", False)
        # Opened before liq0 was recorded; $187,156 was the pool's liquidity at that buy.
        old = self.positions.get("1VJyBvUb83sDBjRvnVH2chd1bVzFCmrrJyFidbCM7KR")
        if old and not old.get("liq0"):
            old["liq0"] = 187_156.0
        tp = DATA / "trades.csv"
        if tp.exists():
            sent = set()
            with tp.open() as f:
                for row in csv.DictReader(f):
                    if row.get("action") == "buy" and row.get("sig"):
                        sent.add(row["mint"])
            unsent = {m for m in self.bought if m not in sent}
            if unsent:
                logger.warning(f"[load] {len(unsent)} coin(s) marked bought with no transaction sent; unmarking")
                self.bought -= unsent
        if not (DATA / "trades.csv").exists():
            # No transaction was ever sent: any "bought" mark or finished selftest is
            # left over from refused quotes, not from trades.
            self.bought.clear()
            self.selftest_done = False

    def save(self) -> None:
        st = {"positions": self.positions, "bought": sorted(self.bought),
              "symbols": sorted(self.symbols), "selftest_done": self.selftest_done}
        tmp = DATA / "state.json.tmp"
        tmp.write_text(json.dumps(st, indent=1))
        tmp.replace(DATA / "state.json")

    def log_trade(self, row: dict) -> None:
        path = DATA / "trades.csv"
        new = not path.exists()
        fields = ["time_utc", "action", "symbol", "mint", "sol", "tokens", "sig", "status", "note"]
        with path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", restval="")
            if new:
                w.writeheader()
            w.writerow({"time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())} | row)

    # ----- swaps
    def quote(self, inp: str, out: str, amount: int, slippage: int) -> dict | None:
        doc = jup(f"{JUP_SWAP}/quote?inputMint={inp}&outputMint={out}&amount={amount}"
                   f"&slippageBps={slippage}&restrictIntermediateTokens=true")
        self.rate_limited = bool(doc and doc.get("_http") == 429)
        if not doc or "outAmount" not in doc:
            logger.warning(f"[quote] {inp[:6]}->{out[:6]} failed: {str(doc)[:200]}")
            return None
        return doc

    def swap(self, q: dict) -> tuple[str | None, str]:
        """Sign and send Jupiter's transaction for quote q; (signature, status)."""
        doc = jup(f"{JUP_SWAP}/swap", {
            "quoteResponse": q, "userPublicKey": self.owner, "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": {"priorityLevelWithMaxLamports": {
                "maxLamports": 300_000, "priorityLevel": "high"}},
        }, timeout=30)
        if not doc or "swapTransaction" not in doc:
            logger.warning(f"[swap] no transaction: {str(doc)[:300]}")
            return None, "NO_TX"
        raw = VersionedTransaction.from_bytes(base64.b64decode(doc["swapTransaction"]))
        signed = VersionedTransaction(raw.message, [self.kp])
        sig = self.chain.send(base64.b64encode(bytes(signed)).decode())
        if not sig:
            return None, "SUBMIT_FAILED"
        status = "UNCONFIRMED"
        for _ in range(30):
            time.sleep(2)
            status = self.chain.status(sig)
            if status != "UNCONFIRMED":
                break
        return sig, status

    def decimals(self, mint: str) -> int | None:
        doc = self.chain.call("getTokenSupply", [mint])
        try:
            return int(doc["result"]["value"]["decimals"])
        except (KeyError, TypeError, ValueError):
            return None

    def fill_cost(self, pool: str | None, mint: str, lamports: int, out_raw: int) -> float | None:
        """1 - quoted tokens / tokens at the pool's price; None if either is unknown."""
        if not pool:
            return None
        doc = feeds.get_json(f"{feeds.DS}/pairs/solana/{pool}", feeds.DS_GATE)
        pair = ((doc or {}).get("pairs") or [None])[0] or (doc or {}).get("pair")
        dec = self.decimals(mint)
        try:
            px_sol = float(pair["priceNative"])  # SOL per whole token
        except (TypeError, KeyError, ValueError):
            return None
        if dec is None or px_sol <= 0:
            return None
        fair = lamports / 1e9 / px_sol * 10**dec
        return 1 - out_raw / fair

    def buy(self, r: dict, size_sol: float, note: str = "") -> dict | None:
        mint, sym = r["mint"], r["symbol"]
        lamports = int(size_sol * 1e9)
        q = self.quote(SOL_MINT, mint, lamports, BUY_SLIPPAGE_BPS)
        if not q:
            logger.warning(f"[buy] {sym} no quote")
            return None
        # Jupiter's priceImpactPct is measured against its own price index, which for
        # a relaunched ticker can be a dead pool's price (95.7% "impact" on a fill
        # that really cost ~1.7%). Judge the fill against the pool's own price instead.
        cost = self.fill_cost(r.get("pool"), mint, lamports, int(q["outAmount"]))
        if cost is None or cost > MAX_FILL_COST:
            shown = "unknown" if cost is None else f"{cost:.1%}"
            logger.warning(f"[buy] {sym} skipped: fill cost vs pool price {shown}")
            return None
        if not ENABLED:
            logger.warning(f"[dry-run] would buy {size_sol} SOL of {sym} {mint} (quote {q['outAmount']} units)")
            return None
        before = self.chain.token_balance(self.owner, mint) or 0
        sig, status = self.swap(q)
        if note != "selftest" and sig:
            self.bought.add(mint)  # one entry per coin once a transaction reached the RPC
            self.save()
        tokens = (self.chain.token_balance(self.owner, mint) or 0) - before
        ok = status == "SUCCESS" or tokens > 0
        self.log_trade({"action": "buy", "symbol": sym, "mint": mint, "sol": size_sol, "tokens": tokens,
                        "sig": sig or "", "status": status, "note": note})
        logger.warning(f"[buy] {sym} {size_sol} SOL -> {tokens} units, {status} {sig}")
        if not ok or tokens <= 0:
            return None
        return {"mint": mint, "symbol": sym, "pool": r.get("pool"), "tokens": tokens,
                "cost_sol": size_sol, "t0": time.time(), "age0_h": r["age_h"], "liq0": r.get("liq"),
                "peak_sol": size_sol, "last_sol": size_sol, "fails": 0}

    def sell(self, pos: dict, why: str) -> bool:
        mint, sym = pos["mint"], pos["symbol"]
        for attempt in range(3):
            held = self.chain.token_balance(self.owner, mint)
            if held == 0:
                logger.warning(f"[sell] {sym} already sold")
                return True
            amount = held if held else pos["tokens"]
            q = self.quote(mint, SOL_MINT, amount, SELL_SLIPPAGE_BPS + 400 * attempt)
            if not q:
                time.sleep(5)
                continue
            sol_before = self.chain.sol_balance(self.owner) or 0
            sig, status = self.swap(q)
            sol_after = self.chain.sol_balance(self.owner) or 0
            got = (sol_after - sol_before) / 1e9
            left = self.chain.token_balance(self.owner, mint)
            self.log_trade({"action": "sell", "symbol": sym, "mint": mint, "sol": round(got, 6),
                            "tokens": amount, "sig": sig or "", "status": status,
                            "note": f"{why}; pnl {got - pos['cost_sol']:+.4f} SOL"})
            logger.warning(f"[sell] {sym} {why}: {status} got {got:+.4f} SOL "
                           f"(cost {pos['cost_sol']}) left {left} {sig}")
            if left == 0:
                return True
            time.sleep(5)
        return False

    # ----- loops
    def candidates(self) -> list[dict]:
        out = []
        for sym in sorted(self.symbols):
            pools = ds_search_pools(sym)
            # A ticker learned from the trending lists is traded only once it has
            # relaunched: TIKTOK matched farm_rule once at 1.3 h and was pulled an
            # hour later. XTEN and PREDICT have relaunched daily all week.
            recent = {r["mint"] for r in pools if r["age_h"] <= RELAUNCH_PROOF_H}
            if sym not in FARM_SEED_SYMBOLS and len(recent) < 2:
                continue
            for r in pools:
                # PumpSwap only: the XTEN/PREDICT pattern is PumpSwap; FLYDOG (Meteora DBC)
                # was pulled while DexScreener kept quoting its last price.
                if r.get("dex") == "pumpswap" and farm_rule(r) and r["mint"] not in self.bought:
                    out.append(r)
        return sorted(out, key=lambda r: r["age_h"])

    def learn(self) -> None:
        snap = feeds.discover()
        for r in snap.values():
            if farm_rule(r) and r["symbol"].upper() not in self.symbols:
                self.symbols.add(r["symbol"].upper())
                logger.warning(f"[learn] new farm ticker {r['symbol']}")

    def pool_liq(self, pool: str | None) -> float | None:
        if not pool:
            return None
        doc = feeds.get_json(f"{feeds.DS}/pairs/solana/{pool}", feeds.DS_GATE)
        pair = ((doc or {}).get("pairs") or [None])[0] or (doc or {}).get("pair")
        try:
            return float(pair["liquidity"]["usd"])
        except (TypeError, KeyError, ValueError):
            return None

    def pool_pair(self, pool: str | None) -> dict | None:
        if not pool:
            return None
        doc = feeds.get_json(f"{feeds.DS}/pairs/solana/{pool}", feeds.DS_GATE)
        return ((doc or {}).get("pairs") or [None])[0] or (doc or {}).get("pair")

    def value_sol(self, pos: dict) -> float | None:
        """Sell value from the pool's own price (DexScreener) less the pool fee.

        Jupiter is kept for the sell itself: the other testers on this machine use
        it too, and its 429s left the trail blind for 20 minutes on the first trade.
        """
        pair = self.pool_pair(pos.get("pool"))
        dec = pos.get("decimals")
        if dec is None:
            dec = self.decimals(pos["mint"])
            pos["decimals"] = dec
        try:
            px = float(pair["priceNative"])
            pos["liq"] = float(pair["liquidity"]["usd"])
            pos.setdefault("liq0", pos["liq"])
        except (TypeError, KeyError, ValueError):
            px = None
        ds_val = pos["tokens"] / 10**dec * px * (1 - POOL_FEE) if px and dec is not None else None
        now = time.time()
        if ds_val is not None and now - pos.get("exec_at", 0) < EXEC_CHECK_EVERY:
            self.rate_limited = False
            return ds_val
        # What a sell would really return. DexScreener can keep quoting a pulled pool.
        pos["exec_at"] = now
        q = self.quote(pos["mint"], SOL_MINT, pos["tokens"], SELL_SLIPPAGE_BPS)
        if q:
            exec_val = int(q["outAmount"]) / 1e9
            pos["exec_sol"] = exec_val
            pos["no_route"] = 0
            return min(exec_val, ds_val) if ds_val is not None else exec_val
        if not self.rate_limited:
            pos["no_route"] = pos.get("no_route", 0) + 1
        return ds_val

    def selftest(self) -> None:
        """One small real buy and sell to prove the swap path before full size."""
        cands = self.candidates()
        if not cands:
            logger.warning("[selftest] no farm coin matching right now; will retry")
            return
        r = cands[0]
        pos = self.buy(r, SELFTEST_SOL, note="selftest")
        if not pos:
            logger.warning("[selftest] buy did not fill; will retry")
            return
        time.sleep(20)
        if self.sell(pos, "selftest"):
            self.selftest_done = True
            logger.warning("[selftest] round trip complete; trading live from here")
        self.save()

    def step(self, now: float, last: dict) -> None:
        if ENABLED and SELFTEST_SOL > 0 and not self.selftest_done:
            self.selftest()
            return
        for mint, pos in list(self.positions.items()):
            if now - pos.get("held_at", 0) >= HELD_EVERY:
                pos["held_at"] = now
                held = self.chain.token_balance(self.owner, mint)
                if held == 0:
                    # Sold outside the bot (e.g. by hand in a wallet app): close it here.
                    logger.warning(f"[close] {pos['symbol']} no longer in the wallet; closing position")
                    self.log_trade({"action": "closed", "symbol": pos["symbol"], "mint": mint,
                                    "note": "sold outside the bot"})
                    self.positions.pop(mint)
                    self.save()
                    continue
                if held is not None and held != pos["tokens"]:
                    pos["tokens"] = held
            age = pos["age0_h"] + (now - pos["t0"]) / 3600
            v = self.value_sol(pos)
            if v is None and self.rate_limited:
                time.sleep(5)  # Jupiter throttling says nothing about the pool; retry
                v = self.value_sol(pos)
            if v is None and not self.rate_limited:
                pos["fails"] += 1
            else:
                pos["fails"] = 0
                pos["last_sol"] = v
                pos["peak_sol"] = max(pos["peak_sol"], v)
            if now - pos.get("liq_at", 0) >= LIQ_EVERY:
                liq = self.pool_liq(pos.get("pool"))
                pos["liq_at"] = now
                if liq:
                    pos.setdefault("liq0", liq)
                    pos["liq0"] = pos["liq0"] or liq
                    pos["liq"] = liq
            why = None
            if pos.get("liq") and pos.get("liq0") and pos["liq"] <= pos["liq0"] * (1 - LIQ_DROP):
                why = f"liquidity {pos['liq']:,.0f} vs {pos['liq0']:,.0f} at entry"
            elif age >= EXIT_AGE_H:
                why = f"age {age:.1f}h"
            elif v is not None and v <= pos["peak_sol"] * (1 - TRAIL):
                why = f"trail: {v:.4f} vs peak {pos['peak_sol']:.4f} SOL"
            elif pos["fails"] >= 10:
                why = "no quote for 5 min"
            elif pos.get("no_route", 0) >= 3:
                why = "no sell route on 3 executable checks"
            if why and self.sell(pos, why):
                self.positions.pop(mint)
            self.save()
        if now - last.get("learn", 0) >= LEARN_EVERY:
            last["learn"] = now
            self.learn()
        if now - last.get("search", 0) >= SEARCH_EVERY and len(self.positions) < MAX_POSITIONS:
            last["search"] = now
            for r in self.candidates():
                if len(self.positions) >= MAX_POSITIONS:
                    break
                bal = self.chain.sol_balance(self.owner) if self.owner else None
                if ENABLED and (bal is None or bal / 1e9 < SIZE_SOL + RESERVE_SOL):
                    shown = "unknown" if bal is None else f"{bal / 1e9:.4f}"
                    logger.warning(f"[buy] skip {r['symbol']}: balance {shown} SOL, need {SIZE_SOL + RESERVE_SOL}")
                    break
                pos = self.buy(r, SIZE_SOL)
                if pos:
                    self.positions[r["mint"]] = pos
                self.save()

    def run(self) -> None:
        bal = self.chain.sol_balance(self.owner) if self.owner else None
        shown = "unknown" if bal is None else f"{bal / 1e9:.4f}"
        logger.warning(f"farmlive {'LIVE' if ENABLED else 'DRY RUN'} wallet {self.owner} "
                       f"balance {shown} SOL size {SIZE_SOL} max {MAX_POSITIONS} "
                       f"open {len(self.positions)} symbols {sorted(self.symbols)}")
        last: dict = {}
        while True:
            try:
                self.step(time.time(), last)
            except Exception:  # noqa: BLE001
                logger.exception("[step] failed")
            time.sleep(WATCH_EVERY)


def main() -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    install_secret_redaction()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = logging.handlers.RotatingFileHandler(DATA / "engine.log", maxBytes=10_000_000, backupCount=3)
    for h in (logging.StreamHandler(sys.stdout), fh):
        h.setFormatter(fmt)
        root.addHandler(h)
    kp = load_keypair() if os.environ.get("SOLANA_PRIVATE_KEY") else None
    if ENABLED and kp is None:
        raise SystemExit("FARMLIVE_ENABLED=1 needs SOLANA_PRIVATE_KEY")
    fl = FarmLive(kp)
    fl.load()
    asyncio.run(asyncio.to_thread(fl.run))


if __name__ == "__main__":
    main()
