"""Paper-trading engine for trending and top-volume Solana tokens.

Runs a population of strategies side by side on the same market feed, paper
trades each one with a fee/slippage/price-impact model, and every few hours
replaces the worst performers with mutations of the best. Moves no funds and
never loads a wallet.

Run: ``python -m trending.engine`` from ``src/``. Settings via env:
TRENDING_DATA_DIR (state, trades.csv, leaderboard.txt), PAPER_SIZE_SOL,
POPULATION, EVOLVE_HOURS.
"""

from __future__ import annotations

import csv
import json
import logging
import logging.handlers
import os
import random
import signal
import statistics
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from trending import feeds
from trending.strategy import (
    FAMILIES,
    LOOSE,
    RANGES,
    SEARCH_FAMILIES,
    crossover,
    describe,
    entry_signal,
    exit_reason,
    mutate,
    random_strategy,
    repair,
)

DATA = Path(os.environ.get("TRENDING_DATA_DIR", "trades/trending"))
SIZE_SOL = float(os.environ.get("PAPER_SIZE_SOL", "0.2"))
POP = int(os.environ.get("POPULATION", "48"))
EVOLVE_H = float(os.environ.get("EVOLVE_HOURS", "3"))
DISCOVER_S = 45  # GeckoTerminal refreshes about once a minute
QUOTE_S = 5  # price checks on open positions
MAX_OPEN = 3  # positions per strategy
FEE_SIDE = 0.006  # DEX fee + priority/network fee, per side
LATENCY_SIDE = 0.004  # price moving between decision and fill, per side
MIN_TRADES_RANKED = 6
CANDIDATE_MIN_TRADES = 60
MIN_PER_FAMILY = 5
# A candidate must spread its trades: riding one or two tokens that happened to
# trend all day is market beta, not a rule that transfers.
CANDIDATE_MIN_TOKENS = 15
CANDIDATE_MAX_TOKEN_SHARE = 0.25
# v2: pools under MIN_POOL_AGE_H are excluded and holds are longer. v1 results
# came almost entirely from fresh launches, so they are archived, not carried.
STATE_VERSION = 2
# Consecutive drained-liquidity reads (QUOTE_S apart) with the price intact
# before the position is closed as "drained" at DRAINED_HAIRCUT of the quote.
# A drained read with the price halving is a rug, closed at 30% at once.
DRAIN_EXIT_TICKS = 12
DRAINED_HAIRCUT = 0.85
# Consecutive reads of a 3x jump on a drained pool before it counts as a rug.
RUG_SPIKE_TICKS = 3
# A ticker seen on another mint within this window is a serial relaunch: PREDICT
# and XTEN came back as a new mint each day, ground up 5%/hour, then dumped 70%.
RELAUNCH_WINDOW_H = 72

logger = logging.getLogger("trending")


def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def side_cost(liq_usd: float, size_usd: float) -> float:
    """Fractional cost of one side: fees, latency, and constant-product impact.

    The quote reserve is about half the pool's USD liquidity, so impact is about
    ``2 * size / liquidity``. Capped at 25% for pools too thin to model.
    """
    impact = 2 * size_usd / liq_usd if liq_usd > 0 else 0.25
    return FEE_SIDE + LATENCY_SIDE + min(impact, 0.25)


def new_stats() -> dict:
    return {
        "n": 0,
        "wins": 0,
        "sum": 0.0,
        "gw": 0.0,
        "gl": 0.0,
        "rets": [],
        "exits": {},
        "mints": {},
    }


def _same_record(a: dict, b: dict) -> bool:
    """Same family and the same trades to within a point of return each.

    Copies born a few minutes apart fill the same token a tick apart, so their
    returns differ in the third decimal; exact equality missed them.
    """
    ra, rb = a["stats"]["rets"], b["stats"]["rets"]
    if a["family"] != b["family"] or abs(len(ra) - len(rb)) > 1:
        return False
    n = min(len(ra), len(rb))
    if n < MIN_TRADES_RANKED:
        return False
    return all(abs(x - y) <= 0.01 for x, y in zip(ra[-n:], rb[-n:], strict=True))


def score(st: dict) -> float:
    """Average net return per trade, shrunk toward zero for small samples."""
    return st["sum"] / (st["n"] + 8)


def profit_factor(st: dict) -> float:
    return st["gw"] / st["gl"] if st["gl"] > 0 else (99.0 if st["gw"] > 0 else 0.0)


def is_candidate(st: dict) -> bool:
    """Enough trades, positive after costs, and positive in both halves."""
    r = st["rets"]
    if st["n"] < CANDIDATE_MIN_TRADES or len(r) < CANDIDATE_MIN_TRADES:
        return False
    h = len(r) // 2
    mints = st.get("mints") or {}
    if len(mints) < CANDIDATE_MIN_TOKENS:
        return False
    if max(mints.values()) > CANDIDATE_MAX_TOKEN_SHARE * sum(mints.values()):
        return False
    return (
        st["sum"] / st["n"] > 0.005
        and profit_factor(st) > 1.25
        and sum(r[:h]) > 0
        and sum(r[h:]) > 0
    )


# Hypotheses from the v1 ledger, restricted to pools 6h+ old: short holds could
# not beat the ~2% round trip, while the few 50-minute holds on top-volume pools
# were positive. These test longer holds and wider targets on established pools.
SEEDS_V2 = [
    {
        "family": "volume_leader",
        "source": "volume",
        "max_rank": 25,
        "min_age_h": 24,
        "m5_min": 0.0,
        "m5_max": 0.05,
        "h1_min": 0.0,
        "h1_max": 0.3,
        "buy_ratio_min": 1.0,
        "min_m5_vol": 5_000,
        "min_liq": 100_000,
        "tp": 0.15,
        "sl": 0.10,
        "use_trail": True,
        "trail_act": 0.08,
        "trail_dist": 0.05,
        "max_hold_min": 90,
    },
    {
        "family": "volume_leader",
        "source": "volume",
        "max_rank": 40,
        "min_age_h": 12,
        "m5_min": -0.02,
        "m5_max": 0.04,
        "h1_min": -0.05,
        "h1_max": 0.5,
        "buy_ratio_min": 0.9,
        "min_m5_vol": 2_000,
        "min_liq": 60_000,
        "tp": 0.25,
        "sl": 0.12,
        "use_trail": False,
        "max_hold_min": 180,
    },
    {
        "family": "dip",
        "source": "any",
        "min_age_h": 12,
        "m5_min": -0.08,
        "m5_max": -0.01,
        "h1_min": 0.05,
        "h1_max": 0.6,
        "buy_ratio_min": 0.8,
        "min_m5_vol": 3_000,
        "min_liq": 80_000,
        "tp": 0.12,
        "sl": 0.10,
        "use_trail": False,
        "max_hold_min": 60,
    },
    {
        "family": "momentum",
        "source": "trend1h",
        "max_rank": 20,
        "min_age_h": 6,
        "m5_min": 0.0,
        "m5_max": 0.04,
        "h1_min": 0.05,
        "h1_max": 0.4,
        "buy_ratio_min": 1.1,
        "min_m5_vol": 5_000,
        "min_liq": 80_000,
        "tp": 0.40,
        "sl": 0.12,
        "use_trail": True,
        "trail_act": 0.10,
        "trail_dist": 0.06,
        "max_hold_min": 120,
    },
    {
        "family": "fresh_trend",
        "source": "trend5m",
        "max_rank": 15,
        "min_age_h": 24,
        "m5_min": 0.0,
        "h1_min": -0.1,
        "h1_max": 1.0,
        "buy_ratio_min": 1.0,
        "min_m5_vol": 5_000,
        "min_liq": 80_000,
        "tp": 0.30,
        "sl": 0.12,
        "use_trail": True,
        "trail_act": 0.12,
        "trail_dist": 0.07,
        "max_hold_min": 90,
    },
    {
        "family": "surge",
        "source": "any",
        "surge_k": 2.0,
        "min_age_h": 24,
        "m5_min": 0.0,
        "h1_min": -0.05,
        "h1_max": 0.6,
        "buy_ratio_min": 1.1,
        "min_m5_vol": 10_000,
        "min_liq": 100_000,
        "tp": 0.20,
        "sl": 0.10,
        "use_trail": True,
        "trail_act": 0.08,
        "trail_dist": 0.05,
        "max_hold_min": 60,
    },
]

# v3 hypotheses. In the first v2 hours the only positive results came from
# ~50-minute holds on top-5 volume pools already up strongly on the day; the
# engine also showed holds under 20 minutes cannot cover the round trip. These
# test whether "established, strongly trending on h6/h24, hold about an hour"
# generalizes beyond the two tokens it was seen on.
SEEDS_V3 = [
    {
        "family": "volume_leader",
        "source": "volume",
        "max_rank": 5,
        "min_age_h": 12,
        "max_age_h": 5_000,
        "m5_min": -0.03,
        "m5_max": 0.05,
        "h1_min": -0.05,
        "h1_max": 0.3,
        "h6_min": 0.2,
        "h24_min": 1.0,
        "buy_ratio_min": 0.6,
        "min_m5_vol": 2_000,
        "min_liq": 100_000,
        "max_mcap": 200_000_000,
        "tp": 0.25,
        "sl": 0.15,
        "use_trail": False,
        "fade_exit": False,
        "max_hold_min": 55,
    },
    {
        "family": "volume_leader",
        "source": "volume",
        "max_rank": 12,
        "min_age_h": 12,
        "max_age_h": 5_000,
        "m5_min": -0.03,
        "m5_max": 0.05,
        "h1_min": -0.05,
        "h1_max": 0.3,
        "h6_min": 0.1,
        "h24_min": 0.3,
        "buy_ratio_min": 0.6,
        "min_m5_vol": 2_000,
        "min_liq": 80_000,
        "max_mcap": 200_000_000,
        "tp": 0.30,
        "sl": 0.15,
        "use_trail": True,
        "trail_act": 0.08,
        "trail_dist": 0.05,
        "fade_exit": False,
        "max_hold_min": 90,
    },
    {
        "family": "momentum",
        "source": "any",
        "min_age_h": 12,
        "max_age_h": 5_000,
        "m5_min": -0.02,
        "m5_max": 0.03,
        "h1_min": 0.03,
        "h1_max": 0.25,
        "h6_min": 0.15,
        "h24_min": 0.5,
        "buy_ratio_min": 0.8,
        "min_m5_vol": 2_000,
        "min_liq": 100_000,
        "max_mcap": 200_000_000,
        "tp": 0.20,
        "sl": 0.12,
        "use_trail": False,
        "fade_exit": False,
        "max_hold_min": 60,
    },
]

# v4 hypotheses from the first v3 night (218 unique fills, 34 tokens): pools
# up 30-100% on the day were the only positive h24 band, holds of 40+ minutes
# were the only positive hold band, and quick stop-loss/flow-fade exits paid
# the round trip for nothing. These test "moderately up on the day, calm 5m,
# hold about an hour with a wide stop" across families.
SEEDS_V4 = [
    {
        "family": "volume_leader",
        "source": "volume",
        "max_rank": 25,
        "min_age_h": 12,
        "max_age_h": 5_000,
        "m5_min": -0.02,
        "m5_max": 0.03,
        "h1_min": -0.05,
        "h1_max": 0.15,
        "h6_min": -0.5,
        "h24_min": 0.3,
        "h24_max": 1.0,
        "buy_ratio_min": 0.6,
        "min_m5_vol": 1_000,
        "min_m5_buyers": 0,
        "pump_only": False,
        "cooldown_min": 30,
        "min_liq": 100_000,
        "max_mcap": 200_000_000,
        "tp": 0.25,
        "sl": 0.25,
        "use_trail": False,
        "fade_exit": False,
        "max_hold_min": 55,
    },
    {
        "family": "momentum",
        "source": "any",
        "min_age_h": 12,
        "max_age_h": 5_000,
        "m5_min": -0.01,
        "m5_max": 0.02,
        "h1_min": 0.0,
        "h1_max": 0.10,
        "h6_min": -0.5,
        "h24_min": 0.3,
        "h24_max": 1.0,
        "buy_ratio_min": 0.6,
        "min_m5_vol": 1_000,
        "min_m5_buyers": 0,
        "pump_only": False,
        "cooldown_min": 30,
        "min_liq": 100_000,
        "max_mcap": 200_000_000,
        "tp": 0.30,
        "sl": 0.20,
        "use_trail": True,
        "trail_act": 0.12,
        "trail_dist": 0.06,
        "fade_exit": False,
        "max_hold_min": 100,
    },
    {
        "family": "dip",
        "source": "any",
        "min_age_h": 12,
        "max_age_h": 5_000,
        "m5_min": -0.05,
        "m5_max": -0.01,
        "h1_min": 0.05,
        "h1_max": 0.20,
        "h6_min": -0.5,
        "h24_min": 0.3,
        "h24_max": 1.5,
        "buy_ratio_min": 0.6,
        "min_m5_vol": 1_000,
        "min_m5_buyers": 0,
        "pump_only": False,
        "cooldown_min": 30,
        "min_liq": 100_000,
        "max_mcap": 200_000_000,
        "tp": 0.15,
        "sl": 0.20,
        "use_trail": False,
        "fade_exit": False,
        "max_hold_min": 60,
    },
]

# v5, from 251 unique fills: chasing (5m up 10%+, top-10 on 5m trending) and
# pools under $100k lost; the stop-loss exits alone (-21% each) were more than
# the whole loss, while 40-60 minute holds on calm entries were positive gross.
# These test calm entries on deep pools with a catastrophe-only stop.
_V5_BASE = {
    "source": "any",
    "max_rank": 30,
    "min_t5_rank": 15,
    "min_age_h": 6,
    "max_age_h": 5_000,
    "m5_min": -0.02,
    "m5_max": 0.02,
    "h1_min": -0.05,
    "h1_max": 0.15,
    "h6_min": -0.2,
    "h24_min": 0.0,
    "h24_max": 2.0,
    "buy_ratio_min": 0.6,
    "min_m5_vol": 1_000,
    "min_m5_buyers": 0,
    "pump_only": False,
    "cooldown_min": 45,
    "min_liq": 100_000,
    "max_mcap": 200_000_000,
    "fade_exit": False,
}
SEEDS_V5 = [
    _V5_BASE
    | {
        "family": "volume_leader",
        "source": "volume",
        "max_rank": 25,
        "tp": 0.25,
        "sl": 0.45,
        "use_trail": False,
        "max_hold_min": 55,
    },
    _V5_BASE
    | {
        "family": "momentum",
        "tp": 0.40,
        "sl": 0.45,
        "use_trail": True,
        "trail_act": 0.15,
        "trail_dist": 0.08,
        "max_hold_min": 120,
    },
    _V5_BASE
    | {
        "family": "momentum",
        "tp": 0.20,
        "sl": 0.25,
        "use_trail": False,
        "max_hold_min": 55,
    },
]

# v6: ex-rug, the only positive slices were entries on pools up 30-100% over 6h
# with a calm 5-minute candle (+2-4% per trade), but nearly all of them came
# from two tokens. These test whether that steady-uptrend drift holds across
# more tokens: any source or top-40 volume, h6 floor +30%, h24 0..+200%.
SEEDS_V6 = [
    _V5_BASE
    | {
        "family": fam,
        "source": src,
        "max_rank": 40,
        "h6_min": 0.3,
        "tp": 0.25,
        "sl": 0.45,
        "use_trail": trail,
        "trail_act": 0.12,
        "trail_dist": 0.06,
        "max_hold_min": hold,
    }
    for fam, src, trail, hold in (
        ("momentum", "any", False, 55),
        ("momentum", "any", False, 120),
        ("momentum", "any", True, 120),
        ("volume_leader", "volume", False, 55),
    )
]

# Seed sets added to a running population once each, in this order.
INJECTED_SEEDS = {"v3": SEEDS_V3, "v4": SEEDS_V4, "v5": SEEDS_V5, "v6": SEEDS_V6}

SEEDS = [
    # Hand-built starting points, one or more per family; evolution takes over.
    {
        "family": "momentum",
        "source": "trend5m",
        "max_rank": 20,
        "m5_min": 0.03,
        "m5_max": 0.25,
        "h1_min": 0.05,
        "h1_max": 1.5,
        "buy_ratio_min": 1.2,
        "min_m5_vol": 15_000,
        "min_liq": 40_000,
        "tp": 0.15,
        "sl": 0.08,
        "use_trail": True,
        "trail_act": 0.06,
        "trail_dist": 0.04,
        "max_hold_min": 30,
    },
    {
        "family": "momentum",
        "source": "volume",
        "max_rank": 30,
        "m5_min": 0.01,
        "m5_max": 0.12,
        "h1_min": 0.0,
        "h1_max": 0.8,
        "buy_ratio_min": 1.1,
        "min_m5_vol": 30_000,
        "min_liq": 100_000,
        "tp": 0.08,
        "sl": 0.05,
        "use_trail": True,
        "trail_act": 0.04,
        "trail_dist": 0.025,
        "max_hold_min": 20,
    },
    {
        "family": "dip",
        "source": "trend1h",
        "max_rank": 20,
        "m5_min": -0.12,
        "m5_max": -0.03,
        "h1_min": 0.25,
        "h1_max": 3.0,
        "buy_ratio_min": 0.9,
        "min_m5_vol": 10_000,
        "min_liq": 50_000,
        "tp": 0.12,
        "sl": 0.08,
        "use_trail": False,
        "max_hold_min": 45,
    },
    {
        "family": "surge",
        "source": "any",
        "surge_k": 2.5,
        "m5_min": 0.0,
        "h1_min": -0.1,
        "h1_max": 2.0,
        "buy_ratio_min": 1.2,
        "min_m5_vol": 20_000,
        "min_liq": 50_000,
        "tp": 0.2,
        "sl": 0.1,
        "use_trail": True,
        "trail_act": 0.08,
        "trail_dist": 0.05,
        "max_hold_min": 25,
    },
    {
        "family": "fresh_trend",
        "source": "trend5m",
        "max_rank": 10,
        "m5_min": 0.0,
        "h1_min": -0.2,
        "h1_max": 3.0,
        "buy_ratio_min": 1.1,
        "min_m5_vol": 10_000,
        "min_liq": 30_000,
        "tp": 0.25,
        "sl": 0.1,
        "use_trail": True,
        "trail_act": 0.1,
        "trail_dist": 0.06,
        "max_hold_min": 40,
    },
    {
        "family": "volume_leader",
        "source": "volume",
        "max_rank": 15,
        "m5_min": 0.005,
        "m5_max": 0.08,
        "h1_min": 0.02,
        "h1_max": 0.6,
        "buy_ratio_min": 1.05,
        "min_m5_vol": 50_000,
        "min_liq": 150_000,
        "tp": 0.06,
        "sl": 0.04,
        "use_trail": True,
        "trail_act": 0.03,
        "trail_dist": 0.02,
        "max_hold_min": 30,
    },
]


class Engine:
    def __init__(self, rng: random.Random | None = None) -> None:
        self.rng = rng or random.Random()
        self.strategies: list[dict] = []
        self.positions: list[dict] = []
        self.cooldown: dict[str, float] = {}
        self.prev_ranks: dict[str, dict] = {}
        self.generation = 0
        self.last_evolve = time.time()
        self.retired: list[dict] = []
        self.candidates: dict[str, dict] = {}
        self.sol_usd = 150.0
        self.started = now_iso()
        self.totals = {"opened": 0, "closed": 0, "snapshots": 0}
        self.version = STATE_VERSION
        self.injected: list[str] = []
        # Ticker (upper case) -> {mint: last seen, epoch seconds}.
        self.symbol_mints: dict[str, dict[str, float]] = {}

    # ---------- population ----------
    def seed(self) -> None:
        self.injected = ["v3", "v4"]
        for spec in SEEDS + SEEDS_V2 + SEEDS_V3 + SEEDS_V4 + SEEDS_V5 + SEEDS_V6:
            s = random_strategy(self.rng, spec["family"])
            s.update(spec)
            repair(s)
            self.strategies.append(self._wrap(s))
        fams = list(SEARCH_FAMILIES)
        while len(self.strategies) < POP:
            fam = fams[len(self.strategies) % len(fams)]
            self.strategies.append(self._wrap(random_strategy(self.rng, fam)))

    def _wrap(self, s: dict) -> dict:
        s["stats"] = new_stats()
        s["born"] = time.time()
        s["gen"] = self.generation
        return s

    def by_id(self) -> dict[str, dict]:
        return {s["id"]: s for s in self.strategies}

    # ---------- trading ----------
    def on_snapshot(self, snap: dict[str, dict], quote_fn) -> None:  # noqa: ANN001
        """Find entries in a discovery snapshot and paper-fill them."""
        self.totals["snapshots"] += 1
        now = time.time()
        self._note_symbols(snap.values(), now)
        relaunch = {r["mint"] for r in snap.values() if self.relaunched(r, now)}
        logged = getattr(self, "_relaunch_logged", set())
        for r in snap.values():
            if r["mint"] in relaunch and r["mint"] not in logged:
                logger.info(f"[relaunch] skipping {r['symbol']} {r['mint']}")
        self._relaunch_logged = logged | relaunch
        open_by_sid = Counter(p["sid"] for p in self.positions)
        held = {(p["sid"], p["mint"]) for p in self.positions}
        wanted: list[tuple[dict, dict]] = []
        recs = sorted(snap.values(), key=lambda r: min(r["ranks"].values() or [99]))
        for s in self.strategies:
            for rec in recs:
                if open_by_sid[s["id"]] >= MAX_OPEN:
                    break
                key = f"{s['id']}:{rec['mint']}"
                if (s["id"], rec["mint"]) in held or self.cooldown.get(key, 0) > now:
                    continue
                if rec["mint"] in relaunch:
                    continue
                if entry_signal(s, rec, self.prev_ranks.get(rec["pool"])):
                    wanted.append((s, rec))
                    open_by_sid[s["id"]] += 1
                    held.add((s["id"], rec["mint"]))
        self.prev_ranks = {p: r["ranks"] for p, r in snap.items()}
        if not wanted:
            return
        q = quote_fn({rec["pool"]: rec["mint"] for _, rec in wanted})
        size_usd = SIZE_SOL * self.sol_usd
        for s, rec in wanted:
            fq = q.get(rec["pool"])
            if not fq:
                continue  # no fresh executable price; skip rather than fill on stale data
            liq = fq["liq"] or rec["liq"]
            # The discovery feed can list a pool that is already drained or whose
            # price disagrees with the live quote; neither is a real fill.
            if (
                liq < max(5_000, 0.5 * s["min_liq"])
                or not 0.7 < fq["price"] / rec["price"] < 1.43
            ):
                continue
            cost = side_cost(liq, size_usd)
            self.positions.append(
                {
                    "sid": s["id"],
                    "pool": rec["pool"],
                    "mint": rec["mint"],
                    "symbol": rec["symbol"],
                    "entry": fq["price"],
                    "entry_eff": fq["price"] * (1 + cost),
                    "peak": fq["price"],
                    "last": fq["price"],
                    "t0": now,
                    "liq0": liq,
                    "missing_since": None,
                    "ctx": {
                        "m5": rec["pc"]["m5"],
                        "h1": rec["pc"]["h1"],
                        "h6": rec["pc"].get("h6"),
                        "h24": rec["pc"]["h24"],
                        "bs5": round(
                            rec["tx"]["m5"]["buys"] / max(rec["tx"]["m5"]["sells"], 1),
                            2,
                        ),
                        "liq": round(liq),
                        "mcap": round(rec["mcap"]),
                        "age_h": round(rec["age_h"], 1),
                        "ranks": rec["ranks"],
                        "dex": rec["dex"],
                    },
                }
            )
            self.totals["opened"] += 1
            logger.info(
                f"[paper] OPEN {s['id']} {s['family']:13} {rec['symbol']:12} "
                f"@ {fq['price']:.8g} liq ${liq:,.0f} cost/side {cost:.2%}"
            )

    def _note_symbols(self, recs, now: float) -> None:  # noqa: ANN001
        cutoff = now - RELAUNCH_WINDOW_H * 3600
        for r in recs:
            sym = str(r.get("symbol", "")).upper()
            if sym and r.get("mint"):
                self.symbol_mints.setdefault(sym, {})[r["mint"]] = now
        pruned = {}
        for sym, mints in self.symbol_mints.items():
            kept = {m: t for m, t in mints.items() if t >= cutoff}
            if kept:
                pruned[sym] = kept
        self.symbol_mints = pruned

    def relaunched(self, rec: dict, now: float) -> bool:
        """True when the ticker was seen on a different mint inside the window."""
        cutoff = now - RELAUNCH_WINDOW_H * 3600
        mints = self.symbol_mints.get(str(rec.get("symbol", "")).upper(), {})
        return any(m != rec["mint"] and t >= cutoff for m, t in mints.items())

    def backfill_symbols(self, path: Path) -> None:
        """Seed the ticker history from the trade ledger after a restart."""
        if self.symbol_mints or not path.exists():
            return
        cutoff = time.time() - RELAUNCH_WINDOW_H * 3600
        try:
            with path.open(newline="") as f:
                for row in csv.DictReader(f):
                    try:
                        t = datetime.strptime(
                            row["time_utc"], "%Y-%m-%dT%H:%M:%SZ"
                        ).replace(tzinfo=UTC).timestamp()
                    except (KeyError, ValueError):
                        continue
                    sym, mint = (row.get("symbol") or "").upper(), row.get("mint")
                    if t >= cutoff and sym and mint:
                        seen = self.symbol_mints.setdefault(sym, {})
                        seen[mint] = max(seen.get(mint, 0.0), t)
        except OSError as e:
            logger.warning(f"ticker backfill failed: {e}")

    def on_quotes(self, q: dict[str, dict]) -> None:
        """Mark open positions to the latest prices and close any that exit."""
        now = time.time()
        strat = self.by_id()
        size_usd = SIZE_SOL * self.sol_usd
        keep = []
        for p in self.positions:
            s = strat.get(p["sid"])
            fq = q.get(p["pool"])
            reason, px = None, None
            if s is None:
                reason, px = "retired", p["last"]
            elif fq is None:
                p["missing_since"] = p["missing_since"] or now
                if now - p["missing_since"] > 300:
                    reason, px = "vanished", p["last"] * 0.5
            else:
                p["missing_since"] = None
                price = fq["price"]
                # A zero is a missing field, not an empty pool. A drained read
                # with the price intact must persist before it counts: real
                # pulls stay pulled, a one-off bad liquidity print does not.
                low = 0 < fq["liq"] < 0.3 * p["liq0"]
                p["drain_ticks"] = p.get("drain_ticks", 0) + 1 if low else 0
                crashed = price < 0.5 * p["last"] or price < 0.5 * p["entry"]
                rugged = low and crashed
                spiked = price > 3 * p["last"] and 0 < fq["liq"] < 0.5 * p["liq0"]
                if not spiked:
                    p["rug_spike_ticks"] = 0
                elif p.get("rug_spike_ticks", 0) < RUG_SPIKE_TICKS - 1:
                    # A 3x print on a drained read is usually a wrong-pool quote
                    # (a tokenized stock reopened at its old price after one); a
                    # real pull stays drained, so it must repeat before it closes.
                    p["rug_spike_ticks"] = p.get("rug_spike_ticks", 0) + 1
                    keep.append(p)
                    continue
                if rugged or spiked:
                    # Pool pulled and price collapsing: the quote is not sellable.
                    reason, px = "rug", min(price, p["last"]) * 0.3
                elif low and p["drain_ticks"] >= DRAIN_EXIT_TICKS:
                    # Liquidity reads drained but the price holds (seen on a
                    # tokenized stock and on a pool that kept its price): sell
                    # at the quote less a haircut for the thin book.
                    reason, px = "drained", min(price, p["last"]) * DRAINED_HAIRCUT
                elif price > 2.5 * p["last"] and p.get("spike_ticks", 0) < 2:
                    # A 2.5x jump inside one tick is almost always a bad print;
                    # believe it only once it has held for three ticks.
                    p["spike_ticks"] = p.get("spike_ticks", 0) + 1
                    keep.append(p)
                    continue
                else:
                    p["spike_ticks"] = 0
                    p["last"] = price
                    p["peak"] = max(p["peak"], price)
                    reason = exit_reason(s, p, price, fq, now)
                    px = price
                    if reason == "take_profit":
                        # Counted as a fill at the target, never at a gap above it.
                        px = min(price, p["entry"] * (1 + s["tp"]))
            if reason is None:
                keep.append(p)
                continue
            liq = (fq or {}).get("liq") or p["liq0"]
            exit_eff = px * (1 - side_cost(liq, size_usd))
            self._close(p, s, reason, px, exit_eff / p["entry_eff"] - 1, now)
        self.positions = keep

    def _close(
        self, p: dict, s: dict | None, reason: str, px: float, net: float, now: float
    ) -> None:
        self.totals["closed"] += 1
        held = (now - p["t0"]) / 60
        if s is not None:
            st = s["stats"]
            st["n"] += 1
            st["wins"] += net > 0
            st["sum"] += net
            st["gw" if net > 0 else "gl"] += abs(net)
            st["rets"] = ([*st["rets"], round(net, 5)])[-400:]
            st["exits"][reason] = st["exits"].get(reason, 0) + 1
            mints = st.setdefault("mints", {})
            mints[p["mint"]] = mints.get(p["mint"], 0) + 1
            self.cooldown[f"{s['id']}:{p['mint']}"] = now + s["cooldown_min"] * 60
            if is_candidate(st) and s["id"] not in self.candidates:
                self.candidates[s["id"]] = {
                    "found": now_iso(),
                    "strategy": self._public(s),
                }
                logger.warning(
                    f"[CANDIDATE] {s['id']} looks profitable: {self._line(s)}"
                )
        logger.info(
            f"[paper] CLOSE {p['sid']} {p['symbol']:12} {reason:14} "
            f"net {net:+.2%} held {held:.1f}m"
        )
        path = DATA / "trades.csv"
        new = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(
                    [
                        "time_utc",
                        "sid",
                        "family",
                        "gen",
                        "symbol",
                        "mint",
                        "pool",
                        "entry",
                        "exit",
                        "reason",
                        "held_min",
                        "net_ret",
                        "net_sol",
                        "ctx",
                    ]
                )
            w.writerow(
                [
                    now_iso(),
                    p["sid"],
                    s["family"] if s else "",
                    s["gen"] if s else "",
                    p["symbol"],
                    p["mint"],
                    p["pool"],
                    f"{p['entry']:.10g}",
                    f"{px:.10g}",
                    reason,
                    f"{held:.1f}",
                    f"{net:.5f}",
                    f"{net * SIZE_SOL:.6f}",
                    json.dumps(p["ctx"], separators=(",", ":")),
                ]
            )

    # ---------- evolution ----------
    def evolve(self) -> None:
        """Replace the weakest strategies with mutants/crossovers of the best."""
        now = time.time()
        mature = [
            s for s in self.strategies if now - s["born"] >= EVOLVE_H * 3600 * 0.9
        ]
        ranked = sorted(
            [s for s in mature if s["stats"]["n"] >= MIN_TRADES_RANKED],
            key=lambda s: score(s["stats"]),
            reverse=True,
        )
        idle = [
            s
            for s in mature
            if s["stats"]["n"] < 2 and now - s["born"] >= 2 * EVOLVE_H * 3600
        ]
        cut = (
            ranked[len(ranked) - int(len(ranked) * 0.35) :] if len(ranked) >= 4 else []
        )
        cut = [s for s in cut if s["id"] not in self.candidates]
        clones = self._clones(mature)
        drop = {s["id"] for s in cut + idle + clones}
        if not drop:
            self.last_evolve = now
            return
        self.generation += 1
        elite = ranked[: max(2, len(ranked) // 4)] or self.strategies
        for s in self.strategies:
            if s["id"] in drop:
                self.retired.append(self._public(s) | {"retired": now_iso()})
        self.retired = self.retired[-500:]
        self.strategies = [s for s in self.strategies if s["id"] not in drop]
        # Keep every searched family alive, so an early lucky streak in one
        # family cannot crowd out the others before they have been tested.
        for fam in SEARCH_FAMILIES:
            members = [s for s in self.strategies if s["family"] == fam]
            best = [s for s in ranked if s["family"] == fam and s["id"] not in drop]
            while len(members) < MIN_PER_FAMILY and len(self.strategies) < POP:
                child = (
                    mutate(self._tournament(best), self.rng)
                    if best and self.rng.random() < 0.7
                    else random_strategy(self.rng, fam)
                )
                for k in ("stats", "born", "gen"):
                    child.pop(k, None)
                child = self._wrap(child)
                self.strategies.append(child)
                members.append(child)
        while len(self.strategies) < POP:
            r = self.rng.random()
            if r < 0.6:
                child = mutate(self._tournament(elite), self.rng)
            elif r < 0.8 and len(elite) >= 2:
                a, b = self.rng.sample(elite, 2)
                child = (
                    crossover(a, b, self.rng)
                    if a["family"] == b["family"]
                    else mutate(a, self.rng)
                )
            else:
                child = random_strategy(self.rng)
            for k in ("stats", "born", "gen"):
                child.pop(k, None)
            self.strategies.append(self._wrap(child))
        self.last_evolve = now
        logger.warning(
            f"[evolve] generation {self.generation}: replaced {len(drop)} "
            f"({len(cut)} weak, {len(idle)} idle, {len(clones)} clones); best: "
            f"{self._line(ranked[0]) if ranked else 'n/a'}"
        )

    def _clones(self, pool: list[dict]) -> list[dict]:
        """Strategies whose trade record duplicates an older one's.

        Mutating a parameter that never binds yields a copy that takes the same
        trades; copies waste population and make one result look like many.
        """
        kept: list[dict] = []
        out = []
        for s in sorted(pool, key=lambda s: s["born"]):
            r = s["stats"]["rets"]
            if len(r) < MIN_TRADES_RANKED:
                continue
            if s["id"] not in self.candidates and any(_same_record(k, s) for k in kept):
                out.append(s)
            else:
                kept.append(s)
        return out

    def _tournament(self, pool: list[dict]) -> dict:
        picks = self.rng.sample(pool, min(3, len(pool)))
        return max(picks, key=lambda s: score(s["stats"]))

    # ---------- reporting / persistence ----------
    def _public(self, s: dict) -> dict:
        st = s["stats"]
        return {k: v for k, v in s.items() if k != "stats"} | {
            "n": st["n"],
            "win_rate": round(st["wins"] / st["n"], 3) if st["n"] else None,
            "avg_net": round(st["sum"] / st["n"], 5) if st["n"] else None,
            "total_net_sol": round(st["sum"] * SIZE_SOL, 5),
            "pf": round(profit_factor(st), 2),
            "exits": st["exits"],
            "desc": describe(s),
        }

    def _line(self, s: dict) -> str:
        st = s["stats"]
        if not st["n"]:
            return f"{s['id']} (no trades) {describe(s)}"
        return (
            f"{s['id']} g{s['gen']} n={st['n']} win {st['wins'] / st['n']:.0%} "
            f"avg {st['sum'] / st['n']:+.2%} PF {profit_factor(st):.2f} "
            f"total {st['sum'] * SIZE_SOL:+.4f} SOL "
            f"tokens {len(st.get('mints') or {})} | {describe(s)}"
        )

    def report(self) -> str:
        lines = [
            f"Trending paper engine  {now_iso()}  started {self.started}",
            f"generation {self.generation}  population {len(self.strategies)}  "
            f"open {len(self.positions)}  paper size {SIZE_SOL} SOL  SOL ${self.sol_usd:.0f}",
            f"totals: {self.totals}",
            "",
        ]
        all_rets = [r for s in self.strategies for r in s["stats"]["rets"]]
        if all_rets:
            lines.append(
                f"all live strategies: {len(all_rets)} trades, avg {statistics.mean(all_rets):+.2%}, "
                f"win {sum(r > 0 for r in all_rets) / len(all_rets):.0%}"
            )
        lines.append("\nBy family (live strategies):")
        for fam in FAMILIES:
            rs = [
                r
                for s in self.strategies
                if s["family"] == fam
                for r in s["stats"]["rets"]
            ]
            if rs:
                lines.append(
                    f"  {fam:14} n={len(rs):5} avg {statistics.mean(rs):+.2%} "
                    f"win {sum(r > 0 for r in rs) / len(rs):.0%}"
                )
        lines.append(
            f"\nCandidates (>= {CANDIDATE_MIN_TRADES} trades, profitable in both halves): "
            f"{len(self.candidates)}"
        )
        strat = self.by_id()
        for sid in self.candidates:
            lines.append(
                "  " + (self._line(strat[sid]) if sid in strat else f"{sid} (retired)")
            )
        lines.append("\nLeaderboard (min 6 trades, by shrunk avg):")
        ranked = sorted(
            [s for s in self.strategies if s["stats"]["n"] >= MIN_TRADES_RANKED],
            key=lambda s: score(s["stats"]),
            reverse=True,
        )
        lines += [f"  {i + 1:2}. {self._line(s)}" for i, s in enumerate(ranked[:20])]
        lines.append("\nWorst 5:")
        lines += [f"      {self._line(s)}" for s in ranked[-5:]]
        return "\n".join(lines)

    def save(self) -> None:
        state = {
            k: getattr(self, k)
            for k in (
                "strategies",
                "positions",
                "cooldown",
                "prev_ranks",
                "generation",
                "last_evolve",
                "retired",
                "candidates",
                "sol_usd",
                "started",
                "totals",
                "version",
                "injected",
                "symbol_mints",
            )
        }
        tmp = DATA / "state.json.tmp"
        tmp.write_text(json.dumps(state))
        tmp.replace(DATA / "state.json")
        (DATA / "leaderboard.txt").write_text(self.report() + "\n")
        (DATA / "candidates.json").write_text(json.dumps(self.candidates, indent=1))

    def load(self) -> bool:
        path = DATA / "state.json"
        if not path.exists():
            return False
        try:
            state = json.loads(path.read_text())
        except (OSError, ValueError) as e:
            logger.warning(f"state unreadable ({e}); starting fresh")
            return False
        for k, v in state.items():
            setattr(self, k, v)
        if state.get("version", 1) < STATE_VERSION:
            self._migrate(state.get("version", 1))
        for s in self.strategies:
            for name in RANGES:
                s.setdefault(name, LOOSE.get(name, RANGES[name][0]))
            s["stats"].setdefault("mints", {})
            repair(s)
        for tag, specs in INJECTED_SEEDS.items():
            if tag not in self.injected:
                self._inject(tag, specs)
        now = time.time()
        self.cooldown = {k: v for k, v in self.cooldown.items() if v > now}
        self.symbol_mints = state.get("symbol_mints") or {}
        self.backfill_symbols(DATA / "trades.csv")
        return True

    def _inject(self, tag: str, specs: list[dict]) -> None:
        """Add hand-built strategies once, replacing clones, then the weakest."""
        clones = {s["id"] for s in self._clones(self.strategies)}
        ranked = sorted(
            self.strategies,
            key=lambda s: (s["id"] not in clones, score(s["stats"])),
        )
        drop = {s["id"] for s in ranked[: len(specs)] if s["id"] not in self.candidates}
        for s in self.strategies:
            if s["id"] in drop:
                self.retired.append(self._public(s) | {"retired": now_iso()})
        self.strategies = [s for s in self.strategies if s["id"] not in drop]
        open_ids = {p["sid"] for p in self.positions}
        for spec in specs:
            s = random_strategy(self.rng, spec["family"])
            s.update(spec)
            repair(s)
            self.strategies.append(self._wrap(s))
        self.injected.append(tag)
        logger.warning(
            f"[inject] {tag}: added {len(specs)} seeds, retired {len(drop)} "
            f"({len(drop & clones)} clones, {len(drop & open_ids)} with open positions)"
        )

    def _migrate(self, old: int) -> None:
        """Bring v1 state to v2: archive results, keep and repair the genomes."""
        old_csv = DATA / "trades.csv"
        if old_csv.exists():
            old_csv.replace(DATA / f"trades_v{old}.csv")
        self.retired = []
        self.candidates = {}
        self.positions = []
        self.cooldown = {}
        self.totals = {"opened": 0, "closed": 0, "snapshots": 0}
        self.started = now_iso()
        ranked = sorted(self.strategies, key=lambda s: score(s["stats"]))
        replace = {s["id"] for s in ranked[: len(SEEDS_V2)]}
        keep = [s for s in self.strategies if s["id"] not in replace]
        self.strategies = []
        for s in keep:
            s.pop("stats", None)
            repair(s)
            self.strategies.append(self._wrap(s))
        for spec in SEEDS_V2:
            s = random_strategy(self.rng, spec["family"])
            s.update(spec)
            repair(s)
            self.strategies.append(self._wrap(s))
        self.last_evolve = time.time()
        self.version = STATE_VERSION
        logger.warning(
            f"[migrate] state v{old} -> v{STATE_VERSION}: results archived to "
            f"trades_v{old}.csv, stats reset, {len(SEEDS_V2)} new seeds"
        )


def main() -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    file_h = logging.handlers.RotatingFileHandler(
        DATA / "engine.log", maxBytes=20_000_000, backupCount=3
    )
    for h in (logging.StreamHandler(sys.stdout), file_h):
        h.setFormatter(fmt)
        root.addHandler(h)

    eng = Engine()
    if eng.load():
        logger.info(
            f"resumed: generation {eng.generation}, {len(eng.strategies)} strategies, "
            f"{len(eng.positions)} open"
        )
    else:
        eng.seed()
        logger.info(f"fresh population of {len(eng.strategies)} strategies")
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))

    next_disc = next_quote = next_save = next_sol = 0.0
    while not stop["flag"]:
        t = time.time()
        if t >= next_sol:
            px = feeds.sol_usd()
            if px:
                eng.sol_usd = px
            next_sol = t + 600
        if t >= next_disc:
            snap = feeds.discover()
            if snap:
                eng.on_snapshot(snap, feeds.quotes)
            else:
                logger.warning("empty discovery snapshot")
            next_disc = t + DISCOVER_S
        if eng.positions and t >= next_quote:
            eng.on_quotes(feeds.quotes({p["pool"]: p["mint"] for p in eng.positions}))
            next_quote = time.time() + QUOTE_S
        if t - eng.last_evolve >= EVOLVE_H * 3600:
            eng.evolve()
        if t >= next_save:
            eng.save()
            logger.info(
                f"[status] gen {eng.generation} open {len(eng.positions)} "
                f"closed {eng.totals['closed']} candidates {len(eng.candidates)}"
            )
            next_save = t + 60
        time.sleep(0.5)
    eng.save()
    logger.info("stopped; state saved")


if __name__ == "__main__":
    main()
