"""Paper test: does copying KOL / smart-money wallets pay, and how?

Wallets come from the bundled ``seed_kols.json`` (472 KolScan KOL wallets with
their X handles and 126 GMGN smart-money wallets, from the public
nirholas/kol-quest data), any listed in ``kols.json`` in the data dir, and,
when SOLBOT_TOKEN is set, the Solana paper bot's share feed (GMGN ``track kol``
and ``track smartmoney``, see Riseofcoyote/Bot ``/api/share/feed``). Each followed wallet is then watched live on
PumpPortal's free ``subscribeAccountTrade`` stream, so a buy is usually seen
within a second or two, well before a caller posts on X.

Theories, each paper-traded on its own price path:

- ``kol_first`` / ``smart_first``: first followed buy on a coin, entered ~2 s
  after we see it.
- ``kol_late60`` / ``smart_late60``: the same signal entered 60 s later, which
  prices what being slow (or waiting for the tweet) costs.
- ``confluence2`` / ``confluence3``: 2 or 3 distinct followed wallets buying the
  same coin within 15 minutes.
- ``control``: a random pump.fun launch every few minutes.

Exit rules scored on every path: the devwatch set, plus ``follow`` (sell when
the wallet that triggered us sells) and ``follow_any`` (sell when any followed
wallet sells that coin). Per wallet it also measures how fast they dump after
buying, and the report re-scores ``kol_first`` with known fast dumpers removed.

Results are pushed back to the Solana bot's inbox every 10 minutes along with
the trending and devwatch reports. Moves no funds and never loads a wallet.
Run ``python -m kolwatch.engine`` from ``src/``. Env: KOLWATCH_DATA_DIR,
SOLBOT_URL, SOLBOT_TOKEN (the Solana bot's DASHBOARD_TOKEN), TRENDING_DATA_DIR,
DEVWATCH_DATA_DIR, SOLANA_NODE_RPC_ENDPOINT (optional).
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import logging.handlers
import os
import random
import statistics
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

from devwatch.engine import (
    DEFAULT_KEY,
    GRAD_SIDE,
    LATENCY,
    SOL_MINT,
    VARIANTS,
    Path_,
    Rpc,
    curve_buy_tokens,
    curve_sell_lamports,
    decode_curve,
    now_iso,
)
from trending import feeds

DATA = Path(os.environ.get("KOLWATCH_DATA_DIR", "trades/kolwatch"))
SIZE_SOL = float(os.environ.get("PAPER_SIZE_SOL", "0.2"))
SOLBOT_URL = os.environ.get("SOLBOT_URL", "https://rock-solana-paper-bot.fly.dev").rstrip("/")
SOLBOT_TOKEN = os.environ.get("SOLBOT_TOKEN", "")
PUMPPORTAL = "wss://pumpportal.fun/api/data"
PUMP_PROGRAM = "6EF8rrecthR5Dzon8Nwu78hRvfCKubJ14M5uBEwF6P"

ENTRY_DELAY = 2.0
LATE_DELAY = 60.0
TRACK_SECONDS = 3600
CONTROL_EVERY = 300
CONFLUENCE_WINDOW = 900
SIGNAL_COOLDOWN = 6 * 3600  # one signal of a kind per coin in this window
MAX_OPEN = 80
MAX_FOLLOW = 400
FEED_EVERY = 60
PUSH_EVERY = 600
DUMP_SECONDS = 300  # a sell this soon after the buy counts as a fast dump
MIN_BUY_SOL = 0.05

EXITS = [name for name, *_ in VARIANTS] + ["follow", "follow_any"]
KINDS = [
    "kol_first",
    "kol_late60",
    "smart_first",
    "smart_late60",
    "confluence2",
    "confluence3",
    "control",
]

logger = logging.getLogger("kolwatch")


def bonding_curve_address(mint: str) -> str | None:
    try:
        from solders.pubkey import Pubkey  # noqa: PLC0415

        pda, _ = Pubkey.find_program_address(
            [b"bonding-curve", bytes(Pubkey.from_string(mint))],
            Pubkey.from_string(PUMP_PROGRAM),
        )
        return str(pda)
    except Exception:  # noqa: BLE001
        return None


def _num(v: object) -> float | None:
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


class CopyPath(Path_):
    """A paper copy trade; adds exits tied to the followed wallets selling."""

    def __init__(self, kind: str, meta: dict) -> None:
        super().__init__(kind, meta)
        self.last = 1.0

    def mark(self, mult: float) -> None:
        self.last = mult
        super().mark(mult)

    def wallet_sold(self, wallet: str) -> None:
        el = time.time() - self.t0
        if wallet == self.meta.get("wallet"):
            self.exits.setdefault("follow", ("kolsell", self.last - 1, el))
        self.exits.setdefault("follow_any", ("kolsell", self.last - 1, el))

    def finish(self) -> None:
        el = time.time() - self.t0
        for name in ("follow", "follow_any"):
            self.exits.setdefault(name, ("end", self.last - 1, el))
        super().finish()


def http_json(url: str, token: str = "", body: dict | None = None) -> dict | None:
    headers = {"Accept": "application/json", "User-Agent": "pumpfun-bot-kolwatch/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:  # noqa: S310
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code}
    except (urllib.error.URLError, TimeoutError, ValueError, OSError) as e:
        return {"_error": type(e).__name__}


class KolWatch:
    def __init__(self, rpc: Rpc) -> None:
        self.rpc = rpc
        self.follow: dict[str, dict] = {}  # wallet -> {kind, name, src}
        self.subscribed: set[str] = set()
        self.feed_seen: set[str] = set()
        self.open_buys: dict[str, dict[str, float]] = defaultdict(dict)  # mint -> wallet -> t
        self.wallet_stats: dict[str, dict] = {}
        self.fired: dict[str, float] = {}  # f"{kind}:{mint}" -> t
        self.open: list[CopyPath] = []
        self.tasks: set[asyncio.Task] = set()  # strong refs; the loop keeps only weak ones
        self.done: list[dict] = []
        self.stats = Counter()
        self.feed_status: dict = {}
        self.lags: list[float] = []
        self.last_control = 0.0
        self.sol_usd = 150.0
        self.started = now_iso()

    # ----- persistence
    def save(self) -> None:
        state = {
            "follow": self.follow,
            "wallet_stats": self.wallet_stats,
            "stats": dict(self.stats),
            "started": self.started,
        }
        tmp = DATA / "state.json.tmp"
        tmp.write_text(json.dumps(state))
        tmp.replace(DATA / "state.json")
        (DATA / "report.txt").write_text(self.report() + "\n")

    def load(self) -> None:
        p = DATA / "state.json"
        if p.exists():
            try:
                st = json.loads(p.read_text())
                self.follow = st.get("follow", {})
                self.wallet_stats = st.get("wallet_stats", {})
                self.stats = Counter(st.get("stats", {}))
                self.started = st.get("started", self.started)
            except (OSError, ValueError) as e:
                logger.warning(f"state unreadable ({e}); fresh start")
        rp = DATA / "results.csv"
        if rp.exists():
            with rp.open() as f:
                self.done = list(csv.DictReader(f))
        self.load_seed()

    def load_seed(self) -> None:
        """Bundled seed_kols.json (KolScan KOLs, GMGN smart money) plus kols.json.

        Both are lists of {"wallet", "name", "kind": "kol"|"smart"}; the data-dir
        kols.json is for wallets added by hand and wins over the bundled list.
        """
        user = DATA / "kols.json"
        if not user.exists():
            user.write_text("[]\n")
        for p in (Path(__file__).with_name("seed_kols.json"), user):
            try:
                rows = json.loads(p.read_text())
            except (OSError, ValueError) as e:
                logger.warning(f"{p.name} unreadable: {e}")
                continue
            for row in rows if isinstance(rows, list) else []:
                w = str(row.get("wallet", "")).strip() if isinstance(row, dict) else ""
                if 32 <= len(w) <= 44:
                    self.follow[w] = {
                        "kind": "smart" if row.get("kind") == "smart" else "kol",
                        "name": str(row.get("name") or "")[:40],
                        "src": "seed",
                    }

    # ----- wallet book-keeping
    def wstat(self, wallet: str) -> dict:
        return self.wallet_stats.setdefault(
            wallet, {"buys": 0, "sells": 0, "dumps": 0, "holds": []}
        )

    def dumper_rate(self, wallet: str) -> float | None:
        s = self.wallet_stats.get(wallet)
        if not s or s["sells"] < 3:
            return None
        return s["dumps"] / s["sells"]

    # ----- signal intake
    def on_trade(
        self,
        wallet: str,
        mint: str,
        side: str,
        via: str,
        seen_lag: float = 0.0,
        sol: float | None = None,
        curve: str | None = None,
    ) -> None:
        info = self.follow.get(wallet)
        if not info or not mint:
            return
        now = time.time()
        if side == "sell":
            t_buy = self.open_buys.get(mint, {}).pop(wallet, None)
            s = self.wstat(wallet)
            s["sells"] += 1
            if t_buy is not None:
                hold = now - t_buy
                s["holds"] = (s["holds"] + [round(hold)])[-50:]
                if hold <= DUMP_SECONDS:
                    s["dumps"] += 1
            for p in self.open:
                if p.meta.get("mint") == mint:
                    p.wallet_sold(wallet)
            return
        if sol is not None and sol < MIN_BUY_SOL:
            self.stats["dust_buys"] += 1
            return
        first_seen = wallet not in self.open_buys.get(mint, {})
        if not first_seen:
            return  # same wallet adding; already counted
        self.open_buys[mint][wallet] = now - seen_lag
        self.wstat(wallet)["buys"] += 1
        self.stats[f"buys_{via}"] += 1
        if via == "gmgn":
            self.lags.append(seen_lag)
            self.lags = self.lags[-500:]
        kind = info["kind"]
        meta = {
            "mint": mint,
            "wallet": wallet,
            "name": info.get("name") or "",
            "via": via,
            "seen_lag_s": round(seen_lag, 1),
            "buy_sol": round(sol, 3) if sol is not None else "",
            "curve": curve,
            "dumper_rate": self.dumper_rate(wallet),
        }
        self.fire(f"{kind}_first", meta, ENTRY_DELAY)
        self.fire(f"{kind}_late60", meta, LATE_DELAY)
        recent = {
            w for w, t in self.open_buys[mint].items() if now - t <= CONFLUENCE_WINDOW
        }
        for n in (2, 3):
            if len(recent) >= n:
                self.fire(f"confluence{n}", meta | {"buyers": len(recent)}, ENTRY_DELAY)

    def fire(self, kind: str, meta: dict, delay: float) -> None:
        key = f"{kind}:{meta['mint']}"
        now = time.time()
        if now - self.fired.get(key, 0) < SIGNAL_COOLDOWN:
            return
        if len(self.open) >= MAX_OPEN:
            self.stats["skipped_capacity"] += 1
            return
        self.fired[key] = now
        if len(self.fired) > 20_000:
            self.fired = {k: v for k, v in self.fired.items() if now - v < SIGNAL_COOLDOWN}
        self.stats[f"signal_{kind}"] += 1
        logger.warning(
            f"[signal] {kind} {meta['mint']} by {meta['wallet'][:8]} {meta.get('name')} "
            f"via {meta['via']} lag {meta['seen_lag_s']}s"
        )
        self.spawn(self.track(kind, dict(meta), delay))

    # ----- the Solana bot's share feed
    async def feed_loop(self) -> None:
        while True:
            if not SOLBOT_TOKEN:
                self.feed_status = {"ok": False, "why": "SOLBOT_TOKEN not set"}
            else:
                doc = await asyncio.to_thread(
                    http_json, f"{SOLBOT_URL}/api/share/feed", SOLBOT_TOKEN
                )
                if doc and "trades" in doc:
                    try:
                        self.ingest_feed(doc)
                    except (KeyError, TypeError, ValueError) as e:
                        logger.warning(f"[feed] bad payload: {type(e).__name__} {e}")
                else:
                    self.feed_status = {"ok": False, "why": doc}
                    logger.warning(f"[feed] unavailable: {doc}")
            await asyncio.sleep(FEED_EVERY)

    def ingest_feed(self, doc: dict) -> None:
        wallets = doc.get("wallets") or {}
        added = 0
        for w, v in wallets.items():
            prev = self.follow.get(w)
            if prev and prev.get("src") == "seed":
                continue
            kind = "kol" if v.get("kind") == "kol" else "smart"
            if not prev:
                added += 1
            self.follow[w] = {"kind": kind, "name": v.get("name") or "", "src": "gmgn"}
        if len(self.follow) > MAX_FOLLOW:
            by_recent = sorted(
                (w for w, v in self.follow.items() if v.get("src") != "seed"),
                key=lambda w: -(wallets.get(w, {}).get("lastAt") or 0),
            )
            for w in by_recent[MAX_FOLLOW:]:
                self.follow.pop(w, None)
        now_ms = time.time() * 1000
        new = 0
        for tr in doc.get("trades") or []:
            key = f"{tr.get('tx')}:{tr.get('wallet')}:{tr.get('side')}:{tr.get('at')}"
            if key in self.feed_seen:
                continue
            self.feed_seen.add(key)
            age = (now_ms - float(tr.get("at") or 0)) / 1000
            if age > 600:
                continue  # stale on first load; only act on fresh trades
            new += 1
            sol = tr.get("sol")
            if sol is None and tr.get("usd") is not None and self.sol_usd:
                sol = float(tr["usd"]) / self.sol_usd
            self.on_trade(tr["wallet"], tr["token"], tr["side"], "gmgn", max(age, 0), sol)
        if len(self.feed_seen) > 50_000:
            self.feed_seen.clear()
        self.feed_status = {
            "ok": True,
            "at": now_iso(),
            "wallets": len(wallets),
            "new_trades": new,
            "gmgn": doc.get("status"),
        }
        self.stats["feed_pulls"] += 1
        if added:
            logger.warning(f"[feed] +{added} wallets, following {len(self.follow)}")

    async def push_loop(self) -> None:
        while True:
            await asyncio.sleep(PUSH_EVERY)
            if not SOLBOT_TOKEN:
                continue
            body = {"from": "pumpfun-bot", "at": now_iso(), "kolwatch": self.summary()}
            for name, env, fn in (
                ("trending", "TRENDING_DATA_DIR", "leaderboard.txt"),
                ("devwatch", "DEVWATCH_DATA_DIR", "report.txt"),
            ):
                p = Path(os.environ.get(env, f"trades/{name}")) / fn
                try:
                    body[name] = p.read_text()[-40_000:]
                except OSError:
                    body[name] = None
            res = await asyncio.to_thread(
                http_json, f"{SOLBOT_URL}/api/share/inbox", SOLBOT_TOKEN, body
            )
            self.stats["pushes_ok" if res and res.get("ok") else "pushes_failed"] += 1

    # ----- PumpPortal: live wallet trades and control launches
    async def pumpportal_loop(self, websockets) -> None:  # noqa: ANN001
        while True:
            self.subscribed = set()
            try:
                async with websockets.connect(PUMPPORTAL, ping_interval=20) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    logger.info("[pumpportal] connected")
                    sync = asyncio.create_task(self.sync_subs(ws))
                    try:
                        async for raw in ws:
                            self.handle_pp(raw)
                    finally:
                        sync.cancel()
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[pumpportal] {type(e).__name__} {e}; reconnecting")
            await asyncio.sleep(3)

    async def sync_subs(self, ws) -> None:  # noqa: ANN001
        while True:
            want = set(self.follow)
            add = sorted(want - self.subscribed)
            for i in range(0, len(add), 50):
                await ws.send(
                    json.dumps({"method": "subscribeAccountTrade", "keys": add[i : i + 50]})
                )
            drop = sorted(self.subscribed - want)
            if drop:
                await ws.send(json.dumps({"method": "unsubscribeAccountTrade", "keys": drop}))
            if add or drop:
                logger.info(f"[pumpportal] following {len(want)} wallets (+{len(add)} -{len(drop)})")
            self.subscribed = want
            await asyncio.sleep(30)

    def handle_pp(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            return
        if not isinstance(msg, dict):
            return
        tx = msg.get("txType")
        trader = str(msg.get("traderPublicKey") or "")
        if tx is None:
            self.stats["pp_other"] += 1
            if self.stats["pp_other"] <= 20:
                logger.warning(f"[pumpportal] reply: {raw[:300]}")
            return
        if tx in ("buy", "sell"):
            self.stats["pp_trades"] += 1
            if trader not in self.follow:
                self.stats["pp_trades_unfollowed"] += 1
        if tx in ("buy", "sell") and trader in self.follow:
            sol = msg.get("solAmount")
            self.on_trade(
                trader,
                str(msg.get("mint") or ""),
                tx,
                "live",
                0.0,
                float(sol) if sol is not None else None,
                msg.get("bondingCurveKey"),
            )
        elif tx == "create" and msg.get("pool", "pump") == "pump":
            self.stats["launches"] += 1
            if time.time() - self.last_control >= CONTROL_EVERY and len(self.open) < MAX_OPEN:
                self.last_control = time.time()
                meta = {
                    "mint": msg.get("mint"),
                    "wallet": "",
                    "name": "",
                    "via": "control",
                    "seen_lag_s": 0,
                    "buy_sol": "",
                    "curve": msg.get("bondingCurveKey"),
                    "dumper_rate": None,
                }
                self.spawn(self.track("control", meta, ENTRY_DELAY))

    # ----- paper trade
    def spawn(self, coro) -> None:  # noqa: ANN001
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    async def track(self, kind: str, meta: dict, delay: float) -> None:
        mint = meta["mint"]
        await asyncio.sleep(delay)
        curve_addr = meta.get("curve") or await asyncio.to_thread(bonding_curve_address, mint)
        stake = SIZE_SOL * 1e9
        cur = None
        if curve_addr:
            data = await self.rpc.account(curve_addr)
            cur = decode_curve(data) if data else None
            if cur and cur["quote_mint"] not in (SOL_MINT, DEFAULT_KEY):
                self.stats["unfillable"] += 1
                return
        tokens = 0.0
        entry_px = None
        if cur and not cur["complete"] and cur["vt"] > 0:
            tokens = curve_buy_tokens(cur["vt"], cur["vq"], stake)
            first = curve_sell_lamports(cur["vt"], cur["vq"], tokens) / stake
            meta["venue"] = "curve"
        else:
            entry_px = (await asyncio.to_thread(feeds.jup_prices, [mint])).get(mint)
            if not entry_px:
                self.stats["unpriced"] += 1
                return
            first = (1 - GRAD_SIDE - LATENCY) ** 2
            meta["venue"] = "amm"
        p = CopyPath(kind, meta)
        self.open.append(p)
        p.mark(first)
        fails = 0
        while time.time() - p.t0 < TRACK_SECONDS and len(p.exits) < len(EXITS):
            el = time.time() - p.t0
            await asyncio.sleep(4 if el < 600 else 15)
            mult = None
            if entry_px is None:
                data = await self.rpc.account(curve_addr)
                cur = decode_curve(data) if data else None
                if cur and not cur["complete"] and cur["vt"] > 0:
                    mult = curve_sell_lamports(cur["vt"], cur["vq"], tokens) / stake
                elif cur and cur["complete"]:
                    px = (await asyncio.to_thread(feeds.jup_prices, [mint])).get(mint)
                    if px:
                        # first post-graduation price anchors the AMM leg
                        entry_px = px
                        p.meta["grad_base"] = p.last
                        p.meta["graduated"] = True
                        mult = p.last
            else:
                px = (await asyncio.to_thread(feeds.jup_prices, [mint])).get(mint)
                if px:
                    base = p.meta.get("grad_base", (1 - GRAD_SIDE - LATENCY) ** 2)
                    mult = base * px / entry_px
            if mult is None:
                fails += 1
                if fails > 40:
                    break
                continue
            fails = 0
            p.mark(mult)
        p.finish()
        self.open.remove(p)
        self.record(p)

    def record(self, p: CopyPath) -> None:
        row = {
            "time_utc": now_iso(),
            "kind": p.kind,
            "mint": p.meta.get("mint"),
            "wallet": p.meta.get("wallet"),
            "name": p.meta.get("name"),
            "via": p.meta.get("via"),
            "seen_lag_s": p.meta.get("seen_lag_s"),
            "buy_sol": p.meta.get("buy_sol"),
            "venue": p.meta.get("venue"),
            "dumper_rate": "" if p.meta.get("dumper_rate") is None else round(p.meta["dumper_rate"], 2),
            "graduated": bool(p.meta.get("graduated")),
            "peak_mult": round(p.peak, 3),
        }
        for sec, name in ((60, "m1"), (300, "m5"), (900, "m15"), (3600, "m60")):
            v = p.at(sec)
            row[f"mult_{name}"] = round(v, 3) if v is not None else ""
        for name in EXITS:
            why, net, el = p.exits[name]
            row[f"{name}_net"] = round(net, 4)
            row[f"{name}_why"] = why
        path = DATA / "results.csv"
        new = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(row))
            if new:
                w.writeheader()
            w.writerow(row)
        self.done.append({k: "" if v is None else str(v) for k, v in row.items()})
        logger.info(
            f"[result] {p.kind} {str(row['mint'])[:8]} peak x{p.peak:.2f} "
            + " ".join(f"{n}={p.exits[n][1]:+.0%}" for n in EXITS)
        )

    # ----- reporting
    @staticmethod
    def _cells(rs: list[dict]) -> list[float]:
        return [statistics.mean(float(r[f"{n}_net"]) for r in rs) for n in EXITS]

    def theory_table(self) -> list[dict]:
        out = []
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in self.done:
            groups[r["kind"]].append(r)
            if r["kind"] in ("kol_first", "smart_first"):
                groups[f"{r['kind']}[{r['via']}]"].append(r)
                dr = _num(r.get("dumper_rate"))
                if dr is not None and dr < 0.5:
                    groups[f"{r['kind']}-no_dumpers"].append(r)
                bs = _num(r.get("buy_sol"))
                if bs is not None and bs >= 2:
                    groups[f"{r['kind']}-big_buy"].append(r)
        for kind, rs in groups.items():
            cells = self._cells(rs)
            best = max(range(len(EXITS)), key=lambda i: cells[i])
            out.append(
                {
                    "theory": kind,
                    "n": len(rs),
                    "win_rate_best": sum(float(r[f"{EXITS[best]}_net"]) > 0 for r in rs) / len(rs),
                    "best_exit": EXITS[best],
                    "best_net": cells[best],
                    "nets": dict(zip(EXITS, cells, strict=True)),
                }
            )
        return sorted(out, key=lambda x: -x["best_net"])

    def wallet_table(self, top: int = 15) -> list[dict]:
        by = defaultdict(list)
        for r in self.done:
            if r["kind"] in ("kol_first", "smart_first") and r.get("wallet"):
                by[r["wallet"]].append(r)
        rows = []
        for w, rs in by.items():
            s = self.wallet_stats.get(w, {})
            holds = s.get("holds") or []
            rows.append(
                {
                    "wallet": w,
                    "name": self.follow.get(w, {}).get("name", ""),
                    "kind": self.follow.get(w, {}).get("kind", ""),
                    "copies": len(rs),
                    "follow_net": statistics.mean(float(r["follow_net"]) for r in rs),
                    "tp50_net": statistics.mean(float(r["tp50_net"]) for r in rs),
                    "median_hold_s": statistics.median(holds) if holds else None,
                    "dump_rate": self.dumper_rate(w),
                }
            )
        rows.sort(key=lambda x: -x["follow_net"])
        return rows[:top] + [r for r in rows[-top:] if r not in rows[:top]]

    def summary(self) -> dict:
        return {
            "started": self.started,
            "following": len(self.follow),
            "open": len(self.open),
            "results": len(self.done),
            "feed": self.feed_status,
            "theories": self.theory_table(),
            "wallets": self.wallet_table(),
            "report": self.report(),
        }

    def report(self) -> str:
        kinds = Counter(v["kind"] for v in self.follow.values())
        lag = statistics.median(self.lags) if self.lags else None
        lines = [
            f"KOL / smart-money copy test (paper)  {now_iso()}  started {self.started}",
            f"following {len(self.follow)} wallets {dict(kinds)}; subscribed live {len(self.subscribed)}",
            f"feed: {self.feed_status}",
            f"GMGN feed median lag behind the chain: {lag if lag is None else round(lag)} s",
            f"counts: {dict(self.stats)}  open: {len(self.open)}  rpc: {dict(self.rpc.calls)}",
            "",
            f"Mean net per trade after costs ({SIZE_SOL} SOL paper stake), by theory and exit:",
            "  theory                      n  " + " ".join(f"{n:>10}" for n in EXITS),
        ]
        for t in self.theory_table():
            lines.append(
                f"  {t['theory'][:26]:26} {t['n']:3}  "
                + " ".join(f"{t['nets'][n]:+10.1%}" for n in EXITS)
            )
        wt = self.wallet_table()
        if wt:
            lines.append("\nWallets (copying their first buy):")
            lines.append("  wallet    name             kind   n  follow   tp50  hold_s  dump%")
            for w in wt:
                hold = "-" if w["median_hold_s"] is None else f"{w['median_hold_s']:.0f}"
                dump = "-" if w["dump_rate"] is None else f"{w['dump_rate']:.0%}"
                lines.append(
                    f"  {w['wallet'][:8]}  {str(w['name'])[:15]:15} {w['kind']:5} {w['copies']:3} "
                    f"{w['follow_net']:+6.1%} {w['tp50_net']:+6.1%} {hold:>7} {dump:>6}"
                )
        return "\n".join(lines)


async def amain() -> None:
    import websockets  # noqa: PLC0415

    DATA.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = logging.handlers.RotatingFileHandler(
        DATA / "engine.log", maxBytes=20_000_000, backupCount=3
    )
    for h in (logging.StreamHandler(sys.stdout), fh):
        h.setFormatter(fmt)
        root.addHandler(h)
    for noisy in ("websockets", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    kw = KolWatch(Rpc())
    kw.load()
    logger.info(
        f"kolwatch starting: {len(kw.follow)} wallets, {len(kw.done)} results, "
        f"feed {'on' if SOLBOT_TOKEN else 'OFF (no SOLBOT_TOKEN)'}"
    )

    async def saver() -> None:
        while True:
            await asyncio.sleep(120)
            try:
                px = await asyncio.to_thread(feeds.sol_usd)
                if px:
                    kw.sol_usd = px
                kw.load_seed()
                kw.save()
                logger.info(
                    f"[status] following {len(kw.follow)} open {len(kw.open)} results {len(kw.done)}"
                )
            except Exception:  # noqa: BLE001
                logger.exception("save failed")

    await asyncio.gather(
        saver(), kw.feed_loop(), kw.push_loop(), kw.pumpportal_loop(websockets)
    )


def main() -> None:
    random.seed()
    asyncio.run(amain())


if __name__ == "__main__":
    main()
