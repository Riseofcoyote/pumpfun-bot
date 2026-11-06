"""Paper test: does trading the hot narrative (meme theme) beat random picks?

Every pump.fun launch on PumpPortal's free create stream is tagged with themes
from its name, ticker and metadata description (fixed keyword lists plus
"emerging" words: a word suddenly common in launch names becomes its own theme,
``w:<word>``). Every few minutes each theme gets a heat reading from its share
of launches in the last hour vs the last day, graduations in the last two hours
and presence in GeckoTerminal's trending lists.

Strategies, each paper-traded and scored under the devwatch exit rules:

- ``leader``: every ~4 min, the best-ranked trending coin in the hottest theme.
- ``leader_vol`` / ``vol_any``: a coin passing the trending tester's best
  volume-leader rule, with and without a hot theme.
- ``leader_any``: the best-ranked trending coin regardless of theme, same
  cadence; the fair comparison for ``leader``.
- ``hot_early`` / ``second_wave`` (new launches in a hot theme, or right after
  one of its coins graduates) are retired: both lost more than random launches.
- ``farm_relaunch`` / ``farm_first``: the XTEN/PREDICT farm-cycle pattern
  (see ``farm_rule``), split by whether the ticker appeared on another mint in
  the last 72 h; one position per coin at a time, sold by coin age 15 h.
- ``cold``: a launch in a theme whose share is falling (one per 5 min); tests
  whether skipping fading themes helps.
- ``control``: a random launch every 5 minutes.

Moves no funds and never loads a wallet. Run ``python -m narrative.engine``
from ``src/``. Env: NARRATIVE_DATA_DIR, PAPER_SIZE_SOL,
SOLANA_NODE_RPC_ENDPOINT (optional fallback RPC).
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import logging.handlers
import os
import random
import re
import statistics
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict, deque
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

DATA = Path(os.environ.get("NARRATIVE_DATA_DIR", "trades/narrative"))
SIZE_SOL = float(os.environ.get("PAPER_SIZE_SOL", "0.2"))
PUMPPORTAL = "wss://pumpportal.fun/api/data"

ENTRY_DELAY = 2.0
TRACK_SECONDS = 3600
MAX_OPEN = 60
HEAT_EVERY = 300
LEADER_EVERY = 225
CONTROL_EVERY = 300
COLD_EVERY = 300
HOT_GAP = 180  # seconds between hot_early buys in one theme
WAVE_WINDOW = 1800
WAVE_MAX = 3
LEADER_COOLDOWN = 6 * 3600
META_TIMEOUT = 4.0
TAG_KEEP_SECONDS = 3 * 86400

THEMES: dict[str, set[str]] = {
    "ai": {"ai", "agent", "agents", "gpt", "agi", "llm", "neural", "claude", "grok",
           "openai", "deepseek", "robot", "robo", "cyber", "sentient", "bot", "android"},
    "politics": {"trump", "maga", "biden", "kamala", "vance", "president", "election",
                 "vote", "usa", "america", "congress", "senate", "tariff", "white",
                 "democrat", "republican", "obama", "putin", "zelensky", "melania", "barron"},
    "celebrity": {"elon", "musk", "kanye", "drake", "taylor", "swift", "mrbeast", "tate",
                  "ishowspeed", "speed", "kardashian", "diddy", "rogan", "snoop", "cz",
                  "saylor", "vitalik", "ansem", "toly"},
    "dog": {"dog", "doge", "inu", "shib", "shiba", "wif", "puppy", "pup", "doggo",
            "corgi", "bonk", "pitbull", "husky"},
    "cat": {"cat", "kitty", "kitten", "meow", "popcat", "mew", "neko", "catwif"},
    "frog": {"frog", "pepe", "toad", "kek", "ribbit"},
    "animal": {"monkey", "ape", "penguin", "hippo", "moodeng", "bear", "bull", "fish",
               "bird", "duck", "goat", "horse", "pig", "rat", "mouse", "hamster",
               "capybara", "panda", "tiger", "lion", "fox", "wolf", "shark", "whale",
               "chill", "guy", "otter", "sloth", "raccoon", "cow", "chicken"},
    "crypto_meta": {"pump", "pumpfun", "sol", "solana", "degen", "wagmi", "ngmi", "moon",
                    "lambo", "rug", "jeet", "chad", "based", "gm", "wen", "ser", "trench",
                    "trenches", "100x", "1000x", "send", "gem"},
    "brainrot": {"skibidi", "rizz", "sigma", "brainrot", "npc", "gyatt", "aura", "cooked",
                 "ohio", "mewing", "goon", "fanum", "delulu", "lowkey", "slay"},
    "money": {"btc", "bitcoin", "eth", "gold", "usd", "dollar", "stock", "fed", "money",
              "cash", "rich", "bank", "billion", "million", "wealth", "printer"},
    "food": {"pizza", "burger", "taco", "banana", "apple", "coffee", "beer", "sushi",
             "cookie", "cake", "noodle", "rice", "bread"},
    "sports": {"nba", "nfl", "soccer", "football", "fifa", "messi", "ronaldo", "lebron",
               "goal", "olympic", "ufc", "boxing", "f1"},
}
KEYWORD_THEME = {w: t for t, ws in THEMES.items() for w in ws}
STOPWORDS = {
    "the", "and", "for", "you", "your", "this", "that", "with", "from", "are", "was",
    "token", "coin", "official", "new", "first", "best", "just", "not", "all", "its",
    "our", "out", "who", "will", "has", "have", "get", "one", "now", "can", "but",
    "community", "only", "join", "telegram", "twitter", "website", "https", "www",
    "com", "http", "fun", "meme", "memecoin", "launch", "launched",
}
WORD = re.compile(r"[a-z0-9]+")

logger = logging.getLogger("narrative")


def words(text: str) -> list[str]:
    """Lowercase alphanumeric words, camelCase split ("CatWifHat" -> cat wif hat)."""
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text or "")
    return WORD.findall(text.lower())


VOWELS = set("aeiouy")
FILLER = {"still", "lets", "their", "made", "make", "they", "them", "what", "when", "here",
          "there", "like", "love", "life", "time", "real", "true", "very", "much", "more",
          "most", "some", "been", "into", "over", "than", "then", "only", "back", "good",
          "big", "little", "king", "queen", "baby", "world", "day", "night", "man", "girl",
          "boy", "guy", "inu", "coin", "token", "pump", "test", "testing", "gas", "bond"}


def is_word(w: str) -> bool:
    """A plausible theme word: letters only, 4+ long, has a vowel, not filler."""
    return (
        len(w) >= 4 and w.isalpha() and bool(VOWELS & set(w))
        and w not in STOPWORDS and w not in FILLER
    )


def themes_for(ws: list[str], emerging: set[str]) -> set[str]:
    out = {KEYWORD_THEME[w] for w in ws if w in KEYWORD_THEME}
    out |= {f"w:{w}" for w in ws if w in emerging}
    return out


def fetch_json(url: str, timeout: float) -> dict | None:
    if not url.startswith("https://"):
        return None
    req = urllib.request.Request(url, headers={"User-Agent": "pumpfun-bot-narrative/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
            doc = json.loads(r.read(200_000).decode("utf-8", "replace"))
            return doc if isinstance(doc, dict) else None
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return None


class Heat:
    """Rolling launch/graduation counts per theme and the emerging-word list."""

    def __init__(self) -> None:
        self.launches: deque[tuple[float, frozenset[str]]] = deque()
        self.word_events: deque[tuple[float, frozenset[str]]] = deque()
        self.grads: deque[tuple[float, frozenset[str]]] = deque()
        self.trending: Counter = Counter()
        self.emerging: set[str] = set()
        self.table: dict[str, dict] = {}

    def add_launch(self, t: float, ws: list[str], tags: set[str], creator: str = "") -> None:
        self.launches.append((t, frozenset(tags)))
        good = frozenset(w for w in ws if is_word(w))
        self.word_events.append((t, good | {f"@{creator}"} if creator and good else good))

    def add_grad(self, t: float, tags: set[str]) -> None:
        self.grads.append((t, frozenset(tags)))

    def recompute(self, now: float) -> None:
        for dq, keep in ((self.launches, 86400), (self.word_events, 86400), (self.grads, 86400)):
            while dq and dq[0][0] < now - keep:
                dq.popleft()
        n24 = max(len(self.launches), 1)
        n1 = max(sum(1 for t, _ in self.launches if t >= now - 3600), 1)
        w24, w1 = Counter(), Counter()
        makers: dict[str, set[str]] = defaultdict(set)
        for t, ws in self.word_events:
            plain = [w for w in ws if not w.startswith("@")]
            w24.update(plain)
            if t >= now - 3600:
                w1.update(plain)
                who = next((w for w in ws if w.startswith("@")), None)
                for w in plain:
                    makers[w].add(who or f"anon{t}")
        # A real emerging theme is used by several different creators; one spammer
        # relaunching the same name is not.
        self.emerging = {
            w for w, c in w1.items()
            if c >= 6 and len(makers[w]) >= 4 and (c / n1) >= 4 * ((w24[w] + 1) / n24)
            and w not in KEYWORD_THEME
        }
        c24, c1 = Counter(), Counter()
        for t, tags in self.launches:
            c24.update(tags)
            if t >= now - 3600:
                c1.update(tags)
        g2 = Counter()
        for t, tags in self.grads:
            if t >= now - 7200:
                g2.update(tags)
        table = {}
        for th in set(c24) | set(c1) | set(g2) | {f"w:{w}" for w in self.emerging}:
            share1, share24 = c1[th] / n1, (c24[th] + 1) / n24
            ratio = share1 / share24 if share24 else 0.0
            hot = c1[th] >= 5 and ratio >= 1.5 and (g2[th] >= 1 or self.trending[th] >= 1)
            if th.startswith("w:") and th[2:] in self.emerging:
                hot = hot or (g2[th] >= 1 or self.trending[th] >= 1)
            cold = c24[th] >= 50 and ratio <= 0.7 and g2[th] == 0
            table[th] = {
                "l1": c1[th], "l24": c24[th], "ratio": round(ratio, 2), "g2": g2[th],
                "trend": self.trending[th], "hot": hot, "cold": cold,
                "score": round(ratio * (1 + g2[th]) * (1 + self.trending[th]), 2),
            }
        self.table = table

    def hot(self) -> list[str]:
        return sorted((t for t, v in self.table.items() if v["hot"]), key=lambda t: -self.table[t]["score"])

    def cold(self) -> set[str]:
        return {t for t, v in self.table.items() if v["cold"]}


def vol_rule(rec: dict) -> bool:
    """The trending tester's best volume-leader rule (zrhx0e): calm, liquid, steady."""
    pc, tx, vol = rec["pc"], rec["tx"]["m5"], rec["vol"]
    r = rec["ranks"].get("volume")
    return (
        r is not None and r <= 25 and rec["liq"] >= 100_000 and rec["mcap"] < 200e6
        and 6 <= rec["age_h"] <= 5000 and -0.02 <= pc["m5"] <= 0.02
        and -0.05 <= pc["h1"] <= 0.15 and pc.get("h6", 0.0) > -0.20
        and 0.0 <= pc["h24"] <= 2.0 and tx["buys"] / max(tx["sells"], 1) >= 0.6
        and vol["m5"] >= 1_000
    )


FARM_MIN_AGE_H = 1.0
FARM_MAX_AGE_H = 12.0  # enter only this young
FARM_EXIT_AGE_H = 15.0  # sell everything once the coin is this old
RELAUNCH_WINDOW_H = 72


def farm_rule(rec: dict) -> bool:
    """The XTEN/PREDICT farm grind: steady ~$150-190k pool, slow climb, bot-steady flow.

    Each relaunch climbs from its first hour (not from hour 6 as first thought)
    and was rugged at 18-24 h. On 12 h of snapshots this matched only those two
    coins: 9 one-hour holds, all positive, median +3.5% after costs.
    """
    pc, tx = rec["pc"], rec["tx"]["m5"]
    bs = tx["buys"] / max(tx["sells"], 1)
    return (
        FARM_MIN_AGE_H <= rec["age_h"] <= FARM_MAX_AGE_H
        and 120_000 <= rec["liq"] <= 250_000 and rec["mcap"] <= 2e6
        and 0.02 <= pc["h1"] <= 0.20 and pc["h24"] <= 2.5
        and abs(pc["m5"]) <= 0.025 and 1.0 <= bs <= 1.25
        and rec.get("quote_is_sol", True)
    )


RESULT_FIELDS = [
    "time_utc", "kind", "symbol", "mint", "theme", "themes", "after", "age_h",
    "age_exit", "px_src", "lost_price", "venue", "graduated", "peak_mult", "mult_m5", "mult_m15", "mult_m60",
] + [f"{n}_{x}" for n, *_ in VARIANTS for x in ("net", "why")]  # fmt: skip


def load_results(path: Path) -> list[dict]:
    """Rows of results.csv, rewritten under RESULT_FIELDS if its header is older.

    Rows were once appended under whatever header the file started with, so a
    file can mix row widths; each row is mapped by its own width (the file's
    header or RESULT_FIELDS) and rows matching neither are dropped and counted.
    """
    with path.open(newline="") as f:
        raw = list(csv.reader(f))
    if not raw:
        return []
    head, body = raw[0], raw[1:]
    rows, bad = [], 0
    for r in body:
        if len(r) == len(RESULT_FIELDS) and (head == RESULT_FIELDS or len(head) != len(r)):
            rows.append(dict(zip(RESULT_FIELDS, r, strict=True)))
        elif len(r) == len(head):
            rows.append(dict(zip(head, r, strict=True)))
        else:
            bad += 1
    if head != RESULT_FIELDS or bad:
        tmp = path.with_suffix(".csv.tmp")
        with tmp.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=RESULT_FIELDS, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in RESULT_FIELDS})
        tmp.replace(path)
        logger.warning(f"[results] rewrote under current header: {len(rows)} kept, {bad} unreadable")
    return [{k: r.get(k, "") for k in RESULT_FIELDS} for r in rows]


class GuardedPath(Path_):
    def __init__(self, kind: str, meta: dict) -> None:
        super().__init__(kind, meta)
        self.last = 1.0

    def mark(self, mult: float) -> None:
        self.last = mult
        super().mark(mult)


class Narrative:
    def __init__(self, rpc: Rpc) -> None:
        self.rpc = rpc
        self.heat = Heat()
        self.tags: dict[str, dict] = {}  # mint -> {t, tags, sym, name}
        self.open: list[GuardedPath] = []
        self.tasks: set[asyncio.Task] = set()
        self.done: list[dict] = []
        self.stats = Counter()
        self.last_hot: dict[str, float] = {}
        self.wave: dict[str, dict] = {}  # theme -> {t, left}
        self.leader_seen: dict[str, float] = {}
        self.vol_seen: dict[str, float] = {}
        self.farm_open: set[str] = set()
        self.sym_mints: dict[str, dict[str, float]] = defaultdict(dict)
        self.last_control = 0.0
        self.last_cold = 0.0
        self.meta_gate = asyncio.Semaphore(4)
        self.sol_usd = 150.0
        self.started = now_iso()

    # ----- persistence
    def save(self) -> None:
        now = time.time()
        self.tags = {m: v for m, v in self.tags.items() if now - v["t"] < TAG_KEEP_SECONDS}
        state = {"stats": dict(self.stats), "started": self.started, "tags": self.tags,
                 "launches": [(t, sorted(x)) for t, x in self.heat.launches],
                 "words": [(t, sorted(x)) for t, x in self.heat.word_events],
                 "grads": [(t, sorted(x)) for t, x in self.heat.grads]}
        tmp = DATA / "state.json.tmp"
        tmp.write_text(json.dumps(state))
        tmp.replace(DATA / "state.json")
        (DATA / "report.txt").write_text(self.report() + "\n")

    def load(self) -> None:
        p = DATA / "state.json"
        if p.exists():
            try:
                st = json.loads(p.read_text())
                self.stats = Counter(st.get("stats", {}))
                self.started = st.get("started", self.started)
                self.tags = st.get("tags", {})
                for key, dq in (("launches", self.heat.launches), ("words", self.heat.word_events),
                                ("grads", self.heat.grads)):
                    for t, x in st.get(key, []):
                        dq.append((t, frozenset(x)))
            except (OSError, ValueError) as e:
                logger.warning(f"state unreadable ({e}); fresh start")
        rp = DATA / "results.csv"
        if rp.exists():
            self.done = load_results(rp)
        self.heat.recompute(time.time())

    def spawn(self, coro) -> None:  # noqa: ANN001
        task = asyncio.create_task(coro)
        self.tasks.add(task)

        def done(t: asyncio.Task) -> None:
            self.tasks.discard(t)
            if not t.cancelled() and t.exception():
                self.stats["task_errors"] += 1
                logger.error("[task] failed", exc_info=t.exception())

        task.add_done_callback(done)

    # ----- launches and graduations
    async def on_create(self, msg: dict) -> None:
        mint = str(msg.get("mint") or "")
        name, sym = str(msg.get("name") or ""), str(msg.get("symbol") or "")
        desc = ""
        uri = str(msg.get("uri") or "")
        if uri:
            async with self.meta_gate:
                doc = await asyncio.to_thread(fetch_json, uri, META_TIMEOUT)
            if doc:
                desc = str(doc.get("description") or "")[:500]
                self.stats["meta_ok"] += 1
            else:
                self.stats["meta_missing"] += 1
        ws = words(f"{name} {sym}")
        tags = themes_for(ws + words(desc), self.heat.emerging)
        now = time.time()
        self.heat.add_launch(now, ws, tags, str(msg.get("traderPublicKey") or ""))
        self.tags[mint] = {"t": now, "tags": sorted(tags), "sym": sym[:16], "name": name[:40]}
        self.stats["launches"] += 1
        if tags:
            self.stats["tagged"] += 1
        meta = {"mint": mint, "symbol": sym[:16], "name": name[:40], "curve": msg.get("bondingCurveKey"),
                "themes": "|".join(sorted(tags))}
        self.decide(tags, meta, now)

    def decide(self, tags: set[str], meta: dict, now: float) -> None:
        if len(self.open) >= MAX_OPEN:
            self.stats["skipped_capacity"] += 1
            return
        hot = [t for t in self.heat.hot() if t in tags]
        # hot_early and second_wave are retired: both did worse than random launches.
        cold = [t for t in tags if t in self.heat.cold()]
        if cold and not hot and now - self.last_cold >= COLD_EVERY:
            self.last_cold = now
            self.fire("cold", meta | {"theme": cold[0]})
        if now - self.last_control >= CONTROL_EVERY:
            self.last_control = now
            self.fire("control", meta | {"theme": meta["themes"]})

    def on_graduation(self, mint: str) -> None:
        info = self.tags.get(mint)
        self.stats["graduations"] += 1
        if not info:
            self.stats["grad_untagged"] += 1
            return
        tags = set(info["tags"])
        self.heat.add_grad(time.time(), tags)
        for th in tags:
            self.wave[th] = {"t": time.time(), "left": WAVE_MAX, "sym": info["sym"]}
        if tags:
            logger.warning(f"[grad] {info['sym']} {mint[:8]} themes {sorted(tags)}")

    def fire(self, kind: str, meta: dict) -> None:
        self.stats[f"signal_{kind}"] += 1
        logger.warning(f"[signal] {kind} {meta.get('symbol')} {str(meta.get('mint'))[:8]} theme {meta.get('theme')}")
        self.spawn(self.track(kind, meta))

    async def pumpportal_loop(self, websockets) -> None:  # noqa: ANN001
        while True:
            try:
                async with websockets.connect(PUMPPORTAL, ping_interval=20) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    await ws.send(json.dumps({"method": "subscribeMigration"}))
                    logger.info("[pumpportal] connected")
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except ValueError:
                            continue
                        if not isinstance(msg, dict):
                            continue
                        tx = msg.get("txType")
                        if tx in ("migrate", "migration"):
                            self.on_graduation(str(msg.get("mint") or ""))
                        elif tx == "create" and msg.get("pool", "pump") == "pump":
                            self.spawn(self.on_create(msg))
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[pumpportal] {type(e).__name__} {e}; reconnecting")
            await asyncio.sleep(3)

    # ----- trending leaders
    def pool_tags(self, rec: dict) -> set[str]:
        info = self.tags.get(rec["mint"])
        if info:
            return set(info["tags"])
        return themes_for(words(rec.get("symbol", "")), self.heat.emerging)

    async def leader_loop(self) -> None:
        await asyncio.sleep(120)
        while True:
            try:
                snap = await asyncio.to_thread(feeds.discover)
                pools = sorted(snap.values(), key=lambda r: min(r["ranks"].values()))
                trend = Counter()
                for r in pools:
                    trend.update(self.pool_tags(r))
                self.heat.trending = trend
                self.heat.recompute(time.time())
                now = time.time()
                fresh = [r for r in pools if now - self.leader_seen.get(r["mint"], 0) > LEADER_COOLDOWN
                         and r.get("quote_is_sol", True) and r["liq"] >= 20_000]
                if fresh:
                    top = fresh[0]
                    self.leader_seen[top["mint"]] = now
                    self.fire("leader_any", {"mint": top["mint"], "pool": top["pool"], "symbol": top["symbol"],
                                             "theme": "|".join(sorted(self.pool_tags(top))), "amm": True})
                hot = self.heat.hot()
                for th in hot:
                    pick = next((r for r in fresh if th in self.pool_tags(r)), None)
                    if pick:
                        self.leader_seen[pick["mint"]] = now
                        self.fire("leader", {"mint": pick["mint"], "pool": pick["pool"], "symbol": pick["symbol"], "theme": th,
                                             "amm": True})
                        break
                else:
                    self.stats["leader_no_pick"] += 1
                # Farm-cycle pattern: one open position per coin, re-entered after each close.
                for r in pools:
                    seen = self.sym_mints[r["symbol"].upper()]
                    seen.setdefault(r["mint"], now)
                    if not farm_rule(r) or r["mint"] in self.farm_open:
                        continue
                    relaunch = any(m != r["mint"] and now - t <= RELAUNCH_WINDOW_H * 3600
                                   for m, t in seen.items())
                    self.farm_open.add(r["mint"])
                    self.fire("farm_relaunch" if relaunch else "farm_first",
                              {"mint": r["mint"], "pool": r["pool"], "symbol": r["symbol"], "amm": True,
                               "theme": "", "age_h": round(r["age_h"], 1),
                               "exit_after_s": (FARM_EXIT_AGE_H - r["age_h"]) * 3600})
                # Volume-leader rule with and without a hot theme: does the theme add anything?
                vol_ok = [r for r in pools if vol_rule(r)
                          and now - self.vol_seen.get(r["mint"], 0) > LEADER_COOLDOWN]
                if vol_ok:
                    top = vol_ok[0]
                    self.vol_seen[top["mint"]] = now
                    self.fire("vol_any", {"mint": top["mint"], "pool": top["pool"], "symbol": top["symbol"],
                                          "theme": "|".join(sorted(self.pool_tags(top))), "amm": True})
                    themed = next((r for r in vol_ok if self.pool_tags(r) & set(hot)), None)
                    if themed:
                        self.vol_seen[themed["mint"]] = now
                        self.fire("leader_vol", {"mint": themed["mint"], "pool": themed["pool"], "symbol": themed["symbol"],
                                                 "theme": "|".join(sorted(self.pool_tags(themed) & set(hot))),
                                                 "amm": True})
            except Exception:  # noqa: BLE001
                logger.exception("[leader] failed")
            await asyncio.sleep(LEADER_EVERY)

    # ----- paper trade
    async def track(self, kind: str, meta: dict) -> None:
        try:
            await self._track(kind, meta)
        finally:
            self.farm_open.discard(meta.get("mint") or "")

    def pool_price(self, pool: str, mint: str) -> float | None:
        q = feeds.quotes({pool: mint}).get(pool)
        return q["price"] if q else None

    async def _track(self, kind: str, meta: dict) -> None:
        mint = meta.get("mint")
        if not mint:
            return
        await asyncio.sleep(ENTRY_DELAY)
        stake = SIZE_SOL * 1e9
        tokens = 0.0
        entry_px = None
        curve_addr = meta.get("curve")
        pool = meta.get("pool")
        if pool:
            entry_px = await asyncio.to_thread(self.pool_price, pool, mint)
            meta["px_src"] = "pool"
            if not entry_px:
                self.stats["unpriced"] += 1
                return
            first = (1 - GRAD_SIDE - LATENCY) ** 2
            meta["venue"] = "amm"
        elif meta.get("amm") or not curve_addr:
            meta["px_src"] = "jup"
            entry_px = (await asyncio.to_thread(feeds.jup_prices, [mint])).get(mint)
            if not entry_px:
                self.stats["unpriced"] += 1
                return
            first = (1 - GRAD_SIDE - LATENCY) ** 2
            meta["venue"] = "amm"
        else:
            cur = None
            for _ in range(4):
                data = await self.rpc.account(curve_addr)
                cur = decode_curve(data) if data else None
                if cur:
                    break
                await asyncio.sleep(1.5)
            if not cur or cur["complete"] or cur["quote_mint"] not in (SOL_MINT, DEFAULT_KEY):
                self.stats["unfillable"] += 1
                return
            tokens = curve_buy_tokens(cur["vt"], cur["vq"], stake)
            if tokens <= 0:
                return
            first = curve_sell_lamports(cur["vt"], cur["vq"], tokens) / stake
            meta["venue"] = "curve"
            meta["px_src"] = "curve"
        p = GuardedPath(kind, meta)
        self.open.append(p)
        p.mark(first)
        fails = 0
        limit = min(TRACK_SECONDS, meta.get("exit_after_s") or TRACK_SECONDS)
        while time.time() - p.t0 < limit and len(p.exits) < len(VARIANTS):
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
                        entry_px = px
                        p.meta["grad_base"] = p.last
                        p.meta["graduated"] = True
                        mult = p.last
            else:
                if pool:
                    px = await asyncio.to_thread(self.pool_price, pool, mint)
                else:
                    px = (await asyncio.to_thread(feeds.jup_prices, [mint])).get(mint)
                if px:
                    base = p.meta.get("grad_base", (1 - GRAD_SIDE - LATENCY) ** 2)
                    mult = base * px / entry_px
                    if p.last > 0 and (mult > 3 * p.last or mult < p.last / 3):
                        pend = p.meta.get("spike")
                        if pend is None or abs(mult / pend - 1) > 0.3:
                            p.meta["spike"] = mult
                            self.stats["price_spikes_held"] += 1
                            mult = None
                        else:
                            p.meta.pop("spike", None)
                    else:
                        p.meta.pop("spike", None)
            if mult is None:
                fails += 1
                if pool and fails >= 10:
                    # The pool stopped quoting: count it as pulled, not frozen at the last price.
                    p.meta["lost_price"] = True
                    self.stats["lost_price"] += 1
                    p.mark(0.0)
                    break
                if fails > 40:
                    p.meta["lost_price"] = True
                    break
                continue
            fails = 0
            p.mark(mult)
        if time.time() - p.t0 >= limit and limit < TRACK_SECONDS:
            p.meta["age_exit"] = True
        p.finish()
        self.open.remove(p)
        self.record(p)

    def record(self, p: GuardedPath) -> None:
        row = {
            "time_utc": now_iso(),
            "kind": p.kind,
            "symbol": p.meta.get("symbol"),
            "mint": p.meta.get("mint"),
            "theme": p.meta.get("theme") or "",
            "themes": p.meta.get("themes") or "",
            "after": p.meta.get("after") or "",
            "age_h": p.meta.get("age_h", ""),
            "age_exit": bool(p.meta.get("age_exit")),
            "px_src": p.meta.get("px_src", ""),
            "lost_price": bool(p.meta.get("lost_price")),
            "venue": p.meta.get("venue"),
            "graduated": bool(p.meta.get("graduated")),
            "peak_mult": round(p.peak, 3),
        }
        for sec, name in ((300, "m5"), (900, "m15"), (3600, "m60")):
            v = p.at(sec)
            row[f"mult_{name}"] = round(v, 3) if v is not None else ""
        for name, *_ in VARIANTS:
            why, net, _el = p.exits[name]
            row[f"{name}_net"] = round(net, 4)
            row[f"{name}_why"] = why
        path = DATA / "results.csv"
        new = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=RESULT_FIELDS)
            if new:
                w.writeheader()
            w.writerow(row)
        self.done.append({k: str(v) for k, v in row.items()})
        logger.info(f"[result] {p.kind} {row['symbol']} {row['theme']} peak x{p.peak:.2f}")

    # ----- reporting
    def report(self) -> str:
        exits = [n for n, *_ in VARIANTS]
        lines = [
            f"Narrative selection test (paper)  {now_iso()}  started {self.started}",
            f"counts: {dict(self.stats)}  open: {len(self.open)}  rpc: {dict(self.rpc.calls)}",
            f"emerging words: {sorted(self.heat.emerging)[:20]}",
            "",
            "Hot themes now (1h launches, ratio vs 24h share, graduations 2h, trending hits):",
        ]
        for th in self.heat.hot()[:10]:
            v = self.heat.table[th]
            lines.append(f"  {th:18} l1 {v['l1']:4} x{v['ratio']:<5} g2 {v['g2']:3} trend {v['trend']:3}")
        lines.append(f"Cold themes now: {sorted(self.heat.cold())[:12]}")
        lines.append("")
        lines.append(f"Per strategy, net per trade after costs ({SIZE_SOL} SOL stake): mean / median / win%")
        lines.append("  strategy        n  peak>2x  " + "  ".join(f"{e:>18}" for e in exits))
        by = defaultdict(list)
        amm_kinds = {"leader", "leader_any", "leader_vol", "vol_any", "farm_relaunch", "farm_first"}
        for r in self.done:
            if r["kind"] in amm_kinds and not r.get("px_src"):
                continue  # priced from Jupiter before pool pricing; stale prints looked like holds
            by[r["kind"]].append(r)
        for kind in ("farm_relaunch", "farm_first", "leader", "leader_any", "leader_vol", "vol_any",
                     "cold", "control",
                     "hot_early", "second_wave"):
            rs = by.get(kind, [])
            if not rs:
                continue
            cells = []
            for e in exits:
                vals = [float(r[f"{e}_net"]) for r in rs]
                cells.append(f"{statistics.mean(vals):+5.0%}/{statistics.median(vals):+4.0%}/{sum(v > 0 for v in vals) / len(vals):3.0%}")
            p2 = sum(float(r["peak_mult"]) >= 2 for r in rs) / len(rs)
            label = f"{kind}*" if kind in ("hot_early", "second_wave") else kind
            lines.append(f"  {label:13} {len(rs):4}  {p2:6.0%}  " + "  ".join(f"{c:>18}" for c in cells))
        lines.append("  (* retired; history only. Trending-coin rows count only pool-priced trades.)")
        th_rows = defaultdict(list)
        for r in self.done:
            if r["kind"] in ("leader", "leader_vol") and r["theme"]:
                th_rows[r["theme"]].append(float(r["trail_net"]))
        if th_rows:
            lines.append("\nBy theme (leader + leader_vol), trail exit mean / n:")
            for th, vals in sorted(th_rows.items(), key=lambda x: -len(x[1]))[:15]:
                lines.append(f"  {th:18} {statistics.mean(vals):+6.1%}  n {len(vals)}")
        return "\n".join(lines)


async def amain() -> None:
    import websockets  # noqa: PLC0415

    DATA.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = logging.handlers.RotatingFileHandler(DATA / "engine.log", maxBytes=20_000_000, backupCount=3)
    for h in (logging.StreamHandler(sys.stdout), fh):
        h.setFormatter(fmt)
        root.addHandler(h)
    for noisy in ("websockets", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    nv = Narrative(Rpc())
    nv.load()
    logger.info(f"narrative starting: {len(nv.done)} results, {len(nv.heat.launches)} launches in memory")

    async def heater() -> None:
        while True:
            await asyncio.sleep(HEAT_EVERY)
            nv.heat.recompute(time.time())
            hot = nv.heat.hot()
            logger.info(f"[heat] hot {hot[:8]} emerging {sorted(nv.heat.emerging)[:10]}")

    async def saver() -> None:
        while True:
            await asyncio.sleep(120)
            try:
                px = await asyncio.to_thread(feeds.sol_usd)
                if px:
                    nv.sol_usd = px
                nv.save()
                logger.info(f"[status] launches {nv.stats['launches']} open {len(nv.open)} results {len(nv.done)}")
            except Exception:  # noqa: BLE001
                logger.exception("save failed")

    await asyncio.gather(heater(), saver(), nv.leader_loop(), nv.pumpportal_loop(websockets))


def main() -> None:
    random.seed()
    asyncio.run(amain())


if __name__ == "__main__":
    main()
