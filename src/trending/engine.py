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
import gzip
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
    BOOLS,
    FAMILIES,
    LOOSE,
    MAX_ENTRY_BUY_RATIO,
    MAX_ENTRY_H1,
    MAX_ENTRY_H24,
    MAX_ENTRY_M5,
    MAX_POOL_AGE_H,
    MIN_LIQ_USD,
    MIN_POOL_AGE_H,
    RANGES,
    SEARCH_FAMILIES,
    crossover,
    describe,
    entry_signal,
    exit_reason,
    h24_capped,
    mutate,
    random_strategy,
    repair,
)

DATA = Path(os.environ.get("TRENDING_DATA_DIR", "trades/trending"))
SIZE_SOL = float(os.environ.get("PAPER_SIZE_SOL", "0.2"))
POP = int(os.environ.get("POPULATION", "48"))
EVOLVE_H = float(os.environ.get("EVOLVE_HOURS", "3"))
DISCOVER_S = 60  # GeckoTerminal refreshes about once a minute; 8 list calls + PUMPSWAP_PAGES each
# PumpSwap top-volume pages added to discovery. The site-wide lists held about
# four 4-12 h pools per snapshot, and after the relaunch block usually none.
PUMPSWAP_PAGES = 2
# The site-wide lists still leave about one 4-12 h pool per snapshot once
# relaunches are dropped, so pump.fun graduations are added as a source: devwatch
# appends each migration to this file, and every GRAD_SCAN_S the ones that
# graduated 4-12 h ago are looked up on DexScreener; up to 30 PumpSwap pools
# with liquidity near the floor join each snapshot (one more GeckoTerminal call).
GRAD_FILE = (
    Path(os.environ.get("DEVWATCH_DATA_DIR", "trades/devwatch")) / "graduates.csv"
)
GRAD_SCAN_S = 300
QUOTE_S = 5  # price checks on open positions
MAX_OPEN = 3  # positions per strategy
FEE_SIDE = 0.006  # DEX fee + priority/network fee, per side
LATENCY_SIDE = 0.004  # price moving between decision and fill, per side
EXEC_NET_FEE = 0.002  # priority/network fee on a sell priced by the router quote
MIN_TRADES_RANKED = 6
CANDIDATE_MIN_TRADES = 60
MIN_PER_FAMILY = 5
# No family may hold more of the population than this; past it, new slots go to
# fresh random strategies of the smallest family. Without the cap the elite of a
# two-token niche (the XTEN/PREDICT grind) bred into two thirds of the population
# and left it idle once that niche closed.
MAX_FAMILY_SHARE = 0.4
# A candidate must spread its trades: riding one or two tokens that happened to
# trend all day is market beta, not a rule that transfers.
# Both count tickers, not mints: a serial relaunch (XTEN, PREDICT) is one bet on
# one farm however many mints it comes back as.
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
# Consecutive drained reads with the price halved before it counts as a rug.
RUG_CRASH_TICKS = 3
# A ticker seen on an OLDER mint within this window is a serial relaunch: PREDICT
# and XTEN came back as a new mint each day, ground up 5%/hour, then dumped 70%.
# Only the newer mint is blocked. Blocking every mint that shares a ticker also
# blocked the originals of every copied ticker (GTAVI on 105 mints, JENSEN on 68):
# by Nov 18, 681 of 1827 tickers were on 2+ mints and 10 of 11 tradable pools per
# snapshot were skipped, so nothing opened for three days.
# The older mint's pool must also be born inside the window before this one: a
# namesake from weeks earlier is a copied ticker, not a daily relaunch. Without
# this, ANT (7.6 h, $1.5M liquidity) was blocked by a 31-day-old mint and GTA6
# by an 84-day-old one, while the farm blockers (UODF, VTC, EXEN, UGOS, USEF)
# were all 17-42 h older.
RELAUNCH_WINDOW_H = 72
# Another mint whose creation time is unknown (state from before creation times
# were kept) counts as older when it was last seen before this pool was created
# plus this margin: a dead farm mint drops off the lists around its relaunch.
RELAUNCH_UNKNOWN_MARGIN_H = 1.0
# The older mint only counts when it was a real market: liquidity at or above
# this while tracked, or a mint the tester traded. Most copies of a hot ticker
# are thin pump.fun launches; counting them blocked 8 of 9 tradable pools per
# snapshot even after the newer-mint rule (DCTY on 20 mints in 3 h, GTA6 on 91).
# A mint seen before liquidity was kept counts only if it is in the ledger.
RELAUNCH_PRIOR_LIQ_USD = 50_000.0
# Relaunched tickers stay tradable up to this pool age. 0 blocks them at any age:
# farmlive's real buys of young relaunches (FLYDOG, AVATARS, BILL, ANTH twice)
# all came back at 0-0.2% of cost within 15 h, and ANTH Azb3 had no Jupiter
# route at 7.8 h while DexScreener still showed $139k liquidity.
RELAUNCH_YOUNG_H = 0.0
# A pool this calm over 1 h and 6 h cannot move enough in one hold to cover the
# ~2% round trip; tokenized stocks, BTC/XMR wrappers and the like sit here.
CALM_H1 = 0.03
CALM_H6 = 0.06
# On a pool younger than the window, GeckoTerminal's h6/h24 change runs from the
# pool's first print, and on PumpSwap graduates that print is a dust trade:
# NOAF (4-6 h) read h24 +346031% and USPR +440000% for hours, so the h24 cap
# skipped every graduate the grad source added. A window whose implied starting
# market cap is below this is a birth artifact and takes the next shorter window.
# Pump.fun graduates start near 400 SOL of market cap (about $29k at $72), so no
# real move since graduation implies less; GOTF (4.5 h, h24 +92408%) implied $5.4k.
BIRTH_MIN_MCAP_USD = 20_000.0

logger = logging.getLogger("trending")


def fix_birth_windows(rec: dict) -> bool:
    """Replace h6/h24 changes that start at a young pool's first print.

    Mutates rec["pc"] in place (h6 -> h1, then h24 -> the fixed h6) and sets
    rec["pc_fixed"]; returns True when anything changed.
    """
    pc = rec.get("pc") or {}
    mcap = rec.get("mcap") or 0.0
    fixed = False
    for w, shorter, hours in (("h6", "h1", 6), ("h24", "h6", 24)):
        v = pc.get(w)
        if v is None or rec.get("age_h", 9999.0) >= hours or mcap <= 0:
            continue
        if v <= -1 or mcap / (1 + v) < BIRTH_MIN_MCAP_USD:
            pc[w] = pc.get(shorter, 0.0)
            fixed = True
    if fixed:
        rec["pc_fixed"] = True
    return fixed


def is_calm(rec: dict) -> bool:
    """True when the pool's 1 h and 6 h moves are both inside the calm band."""
    pc = rec.get("pc") or {}
    return abs(pc.get("h1", 0.0)) < CALM_H1 and abs(pc.get("h6", 0.0)) < CALM_H6


def gate_funnel(recs, relaunch: set[str]) -> dict[str, int]:  # noqa: ANN001
    """Pools in one snapshot left after each hard entry gate, in order.

    Tells "nothing qualifies" apart from "strategies too narrow" when no
    position has opened for hours.
    """
    out = {"pools": 0, "age": 0, "liq": 0, "fresh_ticker": 0, "not_calm": 0, "caps": 0}
    for r in recs:
        out["pools"] += 1
        if not MIN_POOL_AGE_H <= r["age_h"] <= MAX_POOL_AGE_H:
            continue
        out["age"] += 1
        if r["liq"] < MIN_LIQ_USD:
            continue
        out["liq"] += 1
        if r["mint"] in relaunch:
            continue
        out["fresh_ticker"] += 1
        if is_calm(r):
            continue
        out["not_calm"] += 1
        pc, tx = r["pc"], r["tx"]["m5"]
        if (
            pc["m5"] > MAX_ENTRY_M5
            or (h24_capped(r) and pc["h24"] > MAX_ENTRY_H24)
            or pc["h1"] > MAX_ENTRY_H1
            or tx["buys"] / max(tx["sells"], 1) > MAX_ENTRY_BUY_RATIO
        ):
            continue
        out["caps"] += 1
    return out


class GradSource:
    """PumpSwap pools of coins that graduated off pump.fun 4-12 h ago."""

    def __init__(self, path: Path = GRAD_FILE) -> None:
        self.path = path
        self.pools: list[str] = []
        self.next_scan = 0.0
        self.mints_seen = 0

    def recent_mints(self, now: float) -> list[str]:
        """Mints whose graduation falls in the entry age band (with 0.5 h lead)."""
        lo = now - MAX_POOL_AGE_H * 3600
        hi = now - (MIN_POOL_AGE_H - 0.5) * 3600
        mints: dict[str, None] = {}
        try:
            with self.path.open(newline="") as f:
                for row in csv.DictReader(f):
                    try:
                        t = float(row.get("epoch") or 0)
                    except ValueError:
                        continue
                    if lo <= t <= hi and row.get("mint"):
                        mints[row["mint"]] = None
        except OSError:
            return []
        return list(mints)

    def refresh(self, now: float) -> list[str]:
        """Pool list for the next snapshot; rescans at most every GRAD_SCAN_S."""
        if now < self.next_scan:
            return self.pools
        self.next_scan = now + GRAD_SCAN_S
        try:
            mints = self.recent_mints(now)
            self.mints_seen = len(mints)
            self.pools = feeds.pumpswap_pools_of(
                mints, 0.8 * MIN_LIQ_USD, MIN_POOL_AGE_H - 0.5, MAX_POOL_AGE_H
            )[:30]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[grad] scan failed: {type(e).__name__} {e}")
        return self.pools


def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def side_cost(liq_usd: float, size_usd: float) -> float:
    """Fractional cost of one side: fees, latency, and constant-product impact.

    The quote reserve is about half the pool's USD liquidity, so impact is about
    ``2 * size / liquidity``. Capped at 25% for pools too thin to model.
    """
    impact = 2 * size_usd / liq_usd if liq_usd > 0 else 0.25
    return FEE_SIDE + LATENCY_SIDE + min(impact, 0.25)


def ledger_split(path: Path, farm_symbols: set[str], since: float) -> list[str]:
    """Report lines: closed trades since ``since`` split into relaunch tickers and the rest.

    The farm grind (one ticker relaunched on a new mint each day) carries nearly all
    of the population's profit; the rest of the market is the question a
    transferable rule has to answer, so it gets its own line.
    """
    groups: dict[str, list[tuple[float, str]]] = {
        "relaunch tickers": [],
        "everything else": [],
    }
    if not path.exists():
        return []
    try:
        with path.open(newline="") as f:
            for row in csv.DictReader(f):
                if row.get("reason") == "retired":
                    continue
                try:
                    t = datetime.strptime(row["time_utc"], "%Y-%m-%dT%H:%M:%SZ")
                    ret = float(row["net_ret"])
                except (KeyError, TypeError, ValueError):
                    continue
                if t.replace(tzinfo=UTC).timestamp() < since:
                    continue
                sym = (row.get("symbol") or "").upper()
                key = "relaunch tickers" if sym in farm_symbols else "everything else"
                groups[key].append((ret, row.get("mint") or ""))
    except OSError:
        return []
    out = []
    for name, rows in groups.items():
        if not rows:
            out.append(f"  {name:17} n=    0")
            continue
        rets = [r for r, _ in rows]
        gw = sum(r for r in rets if r > 0)
        gl = -sum(r for r in rets if r < 0)
        pf = gw / gl if gl > 0 else 99.0
        out.append(
            f"  {name:17} n={len(rets):5} avg {statistics.mean(rets):+.2%} "
            f"win {sum(r > 0 for r in rets) / len(rets):.0%} PF {pf:.2f} "
            f"tokens {len({m for _, m in rows})}"
        )
    return out


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
        "syms": {},
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
    toks = st.get("syms") or st.get("mints") or {}
    if len(toks) < CANDIDATE_MIN_TOKENS:
        return False
    if max(toks.values()) > CANDIDATE_MAX_TOKEN_SHARE * sum(toks.values()):
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

# v7: through Nov 11 every pool-age band over 12 h lost money outside the
# relaunch tickers, while young pump.fun pools (6-12 h) grinding up broke even,
# and the new tickers OPNW, UODF, USOE and CASHTAG made +2-5% a trade there.
# These test whether a young, steadily rising PumpSwap pool pays on its own,
# without the relaunch history. The farm dumps seen so far came at 12-24 h.
_V7_BASE = _V5_BASE | {
    "min_age_h": 6,
    "max_age_h": 12,
    "pump_only": True,
    "h1_min": 0.0,
    "h6_min": 0.1,
    "tp": 0.25,
    "trail_act": 0.12,
    "trail_dist": 0.06,
}
SEEDS_V7 = [
    _V7_BASE
    | {
        "family": fam,
        "source": src,
        "max_rank": 25,
        "sl": sl,
        "use_trail": trail,
        "max_hold_min": hold,
    }
    for fam, src, sl, trail, hold in (
        ("volume_leader", "volume", 0.45, False, 55),
        ("volume_leader", "volume", 0.20, False, 55),
        ("momentum", "any", 0.45, False, 55),
        ("momentum", "any", 0.45, True, 90),
    )
]

# v8: through Nov 13 returns in 6-12 h PumpSwap pools fell steadily with entry
# age (6-8 h +3.5% a trade, 11-12 h +1.7%) and the farm rugs came after 12 h.
# These copy the leading volume_leader genome into 4-6 h and 4-8 h pools to see
# whether the edge keeps rising toward graduation or the young pools rug.
SEEDS_V8 = [
    _V5_BASE
    | {
        "family": "volume_leader",
        "source": "volume",
        "max_rank": 25,
        "min_age_h": lo,
        "max_age_h": hi,
        "pump_only": True,
        "tp": 0.25,
        "sl": sl,
        "use_trail": False,
        "max_hold_min": 55,
    }
    for lo, hi, sl in ((4, 6, 0.45), (4, 6, 0.20), (4, 8, 0.45))
]

# v9: outside relaunch tickers, entries into 4-11 h pools were the only age band
# that paid (about 300 trades, +1% a trade, positive in both halves) while every
# older band lost; since Nov 15 the lists held no such pool at all. These take the
# leading calm-entry genome to pools of fresh pump.fun graduations (source
# "grad"), plus one on any source so graduations listed elsewhere count too.
_V9_BASE = _V5_BASE | {"min_age_h": 4, "max_age_h": 12, "pump_only": True}
SEEDS_V9 = [
    _V9_BASE
    | {
        "family": fam,
        "source": src,
        "max_rank": 30,
        "tp": tp,
        "sl": sl,
        "use_trail": trail,
        "trail_act": 0.12,
        "trail_dist": 0.06,
        "max_hold_min": hold,
    }
    for fam, src, tp, sl, trail, hold in (
        ("momentum", "grad", 0.25, 0.45, False, 55),
        ("momentum", "grad", 0.25, 0.20, False, 55),
        ("momentum", "grad", 0.40, 0.30, True, 90),
        ("momentum", "any", 0.25, 0.45, False, 55),
    )
]

# v10: from Nov 19 to Nov 26 nothing opened. The 4-12 h pools that cleared the
# liquidity floor (BASKET, UBR, UBI) were all skipped by the 200% h24 cap, which
# on a pool that young measures the rise since its first print. These copy the v9
# graduate genomes and the leading 6-12 h volume_leader with young_h24_free set,
# to test whether young graduates that have already run pay; the other genomes
# keep the cap, so the two sets compare in the same market.
SEEDS_V10 = [s | {"young_h24_free": True} for s in SEEDS_V9] + [
    _V9_BASE
    | {
        "family": "volume_leader",
        "source": "volume",
        "max_rank": 25,
        "min_age_h": 6,
        "max_age_h": 12,
        "tp": 0.25,
        "sl": sl,
        "use_trail": False,
        "max_hold_min": 55,
        "young_h24_free": True,
    }
    for sl in (0.45, 0.20)
]

# v11: with every 4-12 h pool over the floor blocked as a relaunch, nothing opened
# for a week. These copy the v10 genomes onto 2-6 h graduates, the youngest band
# the age floor allows; the rest of the population keeps its own age range.
SEEDS_V11 = [s | {"min_age_h": 2, "max_age_h": 6} for s in SEEDS_V10]

# Seed sets added to a running population once each, in this order.
INJECTED_SEEDS = {
    "v3": SEEDS_V3,
    "v4": SEEDS_V4,
    "v5": SEEDS_V5,
    "v6": SEEDS_V6,
    "v7": SEEDS_V7,
    "v8": SEEDS_V8,
    "v9": SEEDS_V9,
    "v10": SEEDS_V10,
    "v11": SEEDS_V11,
}

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
        # Mint -> creation of its oldest pool seen, epoch seconds.
        self.mint_created: dict[str, float] = {}
        # Mint -> highest pool liquidity seen, USD.
        self.mint_liq: dict[str, float] = {}
        # Mints with a paper trade in the ledger; rebuilt on load, not saved.
        self.traded_mints: set[str] = set()

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
        seen = getattr(self, "_birth_logged", set())
        fixed = [r for r in snap.values() if fix_birth_windows(r)]
        new = [r for r in fixed if r["mint"] not in seen]
        if new:
            logger.info(
                "[birth] h6/h24 from first print replaced: "
                + ", ".join(f"{r['symbol']} {r['age_h']:.1f}h" for r in new[:8])
            )
        self._birth_logged = seen | {r["mint"] for r in new}
        self._note_symbols(snap.values(), now)
        # A relaunched ticker is skipped once it is past RELAUNCH_YOUNG_H. Paper
        # fills on young relaunches looked steady; real fills on them did not.
        relaunch = {
            r["mint"]
            for r in snap.values()
            if self.relaunched(r, now) and r["age_h"] > RELAUNCH_YOUNG_H
        }
        logged = getattr(self, "_relaunch_logged", set())
        for r in snap.values():
            if r["mint"] in relaunch and r["mint"] not in logged:
                logger.info(f"[relaunch] skipping {r['symbol']} {r['mint']}")
        self._relaunch_logged = logged | relaunch
        self.funnel = gate_funnel(snap.values(), relaunch)
        self.log_relaunch_gate(snap.values(), relaunch, now)
        calm = [
            f"{r['symbol']} h1 {r['pc'].get('h1', 0):+.1%} h6 {r['pc'].get('h6', 0):+.1%}"
            for r in snap.values()
            if MIN_POOL_AGE_H <= r["age_h"] <= MAX_POOL_AGE_H
            and r["liq"] >= MIN_LIQ_USD
            and r["mint"] not in relaunch
            and is_calm(r)
        ]
        if calm:
            logger.info(f"[calm] skipping {len(calm)}: {'; '.join(calm[:8])}")
        capped = []
        for r in snap.values():
            if not (
                MIN_POOL_AGE_H <= r["age_h"] <= MAX_POOL_AGE_H
                and r["liq"] >= MIN_LIQ_USD
                and r["mint"] not in relaunch
                and not is_calm(r)
            ):
                continue
            pc, tx = r["pc"], r["tx"]["m5"]
            why = [
                f"{k} {v:+.0%}"
                for k, v, cap in (
                    ("m5", pc["m5"], MAX_ENTRY_M5),
                    ("h1", pc["h1"], MAX_ENTRY_H1),
                    ("h24", pc["h24"], MAX_ENTRY_H24 if h24_capped(r) else 1e18),
                )
                if v > cap
            ]
            ratio = tx["buys"] / max(tx["sells"], 1)
            if ratio > MAX_ENTRY_BUY_RATIO:
                why.append(f"bs5 {ratio:.2f}")
            if why:
                capped.append(f"{r['symbol']} {r['age_h']:.1f}h {' '.join(why)}")
        if capped:
            logger.info(f"[caps] skipping {len(capped)}: {'; '.join(capped[:8])}")
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
                if rec["mint"] in relaunch or is_calm(rec):
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
                        "pcfix": bool(rec.get("pc_fixed")),
                        "h24free": not h24_capped(rec, s),
                    },
                }
            )
            self.totals["opened"] += 1
            self.traded_mints.add(rec["mint"])
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
                if r.get("age_h") is not None:
                    born = now - float(r["age_h"]) * 3600
                    old = self.mint_created.get(r["mint"], born)
                    self.mint_created[r["mint"]] = min(old, born)
                liq = float(r.get("liq") or 0.0)
                if liq > self.mint_liq.get(r["mint"], 0.0):
                    self.mint_liq[r["mint"]] = liq
        pruned = {}
        for sym, mints in self.symbol_mints.items():
            kept = {m: t for m, t in mints.items() if t >= cutoff}
            if kept:
                pruned[sym] = kept
        self.symbol_mints = pruned
        live = {m for mints in pruned.values() for m in mints}
        self.mint_created = {m: t for m, t in self.mint_created.items() if m in live}
        self.mint_liq = {m: v for m, v in self.mint_liq.items() if m in live}

    def relaunched(self, rec: dict, now: float) -> bool:
        """True when the ticker was on an older, real mint inside the window.

        The oldest mint of a copied ticker is not a relaunch, and neither is a
        mint whose only older namesakes were thin copies (see RELAUNCH_PRIOR_LIQ_USD).
        """
        return self.relaunch_blocker(rec, now) is not None

    def relaunch_blocker(self, rec: dict, now: float) -> str | None:
        """The older real mint that makes ``rec`` a relaunch, or None.

        Ages compare pool creation times (DexScreener pairCreatedAt), not mint
        creation: a pump.fun coin's PumpSwap pool is born at graduation.
        """
        cutoff = now - RELAUNCH_WINDOW_H * 3600
        mints = self.symbol_mints.get(str(rec.get("symbol", "")).upper(), {})
        created = self.mint_created.get(rec["mint"], now - rec["age_h"] * 3600)
        for m, seen in mints.items():
            if m == rec["mint"] or seen < cutoff or not self.real_market(m):
                continue
            other = self.mint_created.get(m)
            if other is not None:
                if created - RELAUNCH_WINDOW_H * 3600 < other < created:
                    return m
            elif seen < created + RELAUNCH_UNKNOWN_MARGIN_H * 3600:
                return m
        return None

    def log_relaunch_gate(self, recs, relaunch: set[str], now: float) -> None:  # noqa: ANN001
        """Log once per mint each pool past the age and liquidity gates that the
        relaunch block skips, with the older mint that blocked it."""
        logged = getattr(self, "_gate_logged", set())
        for r in recs:
            if r["mint"] not in relaunch or r["mint"] in logged:
                continue
            if not MIN_POOL_AGE_H <= r["age_h"] <= MAX_POOL_AGE_H:
                continue
            if r["liq"] < MIN_LIQ_USD:
                continue
            logged.add(r["mint"])
            m = self.relaunch_blocker(r, now)
            if m is None:
                continue
            born = self.mint_created.get(m)
            seen = self.symbol_mints.get(str(r["symbol"]).upper(), {}).get(m, now)
            logger.info(
                f"[relaunch-gate] {r['symbol']} {r['mint']} pool {r['age_h']:.1f}h "
                f"liq ${r['liq']:,.0f} {r.get('dex', '')} blocked by {m} "
                f"pool {'?' if born is None else f'{(now - born) / 3600:.1f}h'} "
                f"peak liq ${self.mint_liq.get(m, 0.0):,.0f} "
                f"traded {m in self.traded_mints} seen {(now - seen) / 3600:.1f}h ago"
            )
        self._gate_logged = logged

    def real_market(self, mint: str) -> bool:
        """True for a mint that held RELAUNCH_PRIOR_LIQ_USD or was traded here."""
        if mint in self.traded_mints:
            return True
        return self.mint_liq.get(mint, 0.0) >= RELAUNCH_PRIOR_LIQ_USD

    def load_traded_mints(self, path: Path) -> None:
        """Every mint in the trade ledger."""
        try:
            with path.open(newline="") as f:
                self.traded_mints = {
                    r["mint"] for r in csv.DictReader(f) if r.get("mint")
                }
        except (OSError, KeyError) as e:
            logger.warning(f"traded-mint load failed: {e}")

    def backfill_symbols(self, path: Path) -> None:
        """Seed the ticker history from the trade ledger after a restart."""
        if self.symbol_mints or not path.exists():
            return
        cutoff = time.time() - RELAUNCH_WINDOW_H * 3600
        try:
            with path.open(newline="") as f:
                for row in csv.DictReader(f):
                    try:
                        t = (
                            datetime.strptime(row["time_utc"], "%Y-%m-%dT%H:%M:%SZ")
                            .replace(tzinfo=UTC)
                            .timestamp()
                        )
                    except (KeyError, ValueError):
                        continue
                    sym, mint = (row.get("symbol") or "").upper(), row.get("mint")
                    if t >= cutoff and sym and mint:
                        seen = self.symbol_mints.setdefault(sym, {})
                        seen[mint] = max(seen.get(mint, 0.0), t)
        except OSError as e:
            logger.warning(f"ticker backfill failed: {e}")

    def backfill_tickers(self, path: Path) -> None:
        """Rebuild per-strategy ticker counts from the ledger for older state."""
        need = {s["id"]: s for s in self.strategies if not s["stats"].get("syms")}
        need = {k: s for k, s in need.items() if s["stats"]["n"]}
        if not need or not path.exists():
            return
        counts: dict[str, dict[str, int]] = {}
        try:
            with path.open(newline="") as f:
                for row in csv.DictReader(f):
                    sid = row.get("sid")
                    if sid in need:
                        sym = (row.get("symbol") or row.get("mint") or "?").upper()
                        c = counts.setdefault(sid, {})
                        c[sym] = c.get(sym, 0) + 1
        except OSError as e:
            logger.warning(f"ticker count backfill failed: {e}")
            return
        for sid, c in counts.items():
            need[sid]["stats"]["syms"] = c

    def exec_floor(
        self, p: dict, sell_fn, cache: dict[str, float | None]
    ) -> float | None:
        """Net return a real sell would get now, from an executable sell quote.

        ``None`` when the mint's decimals are unknown or the router gives no
        answer. A quote that finds no route or pays nothing counts as a total
        loss: farmlive's live sells into pulled young PumpSwap pools paid 0-0.2%
        of cost while DexScreener still showed the old price and liquidity.
        """
        if sell_fn is None or not self.sol_usd:
            return None
        dec = feeds.DECIMALS.get(p["mint"])
        if dec is None:
            return None
        if p["mint"] not in cache:
            tokens = SIZE_SOL * self.sol_usd / p["entry_eff"]
            raw = int(tokens * 10**dec)
            try:
                out = sell_fn(p["mint"], raw)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[exec] sell quote {p['symbol']} failed: {e}")
                cache[p["mint"]] = None
                return None
            if out is None:
                # No route at all, or a failed call: only a pool that reads
                # drained is taken as unsellable, so a router blip is not a rug.
                fq_liq = p.get("liq_now") or p["liq0"]
                cache[p["mint"]] = -0.99 if fq_liq < 0.3 * p["liq0"] else None
            else:
                got = out / 1e9 * (1 - LATENCY_SIDE - EXEC_NET_FEE)
                cache[p["mint"]] = got / SIZE_SOL - 1
        return cache[p["mint"]]

    def on_quotes(self, q: dict[str, dict], sell_fn=None) -> None:
        """Mark open positions to the latest prices and close any that exit.

        ``sell_fn(mint, raw_amount) -> lamports | None`` prices each exit at an
        executable quote; the recorded net is the lower of that and the model.
        """
        now = time.time()
        exec_cache: dict[str, float | None] = {}
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
                if not rugged:
                    p["rug_crash_ticks"] = 0
                elif p.get("rug_crash_ticks", 0) < RUG_CRASH_TICKS - 1:
                    # One drained, crashed print is often a wrong-pool quote (a
                    # tokenized stock read at a fraction of a cent); a real pull
                    # stays pulled, so it must repeat before it closes.
                    p["rug_crash_ticks"] = p.get("rug_crash_ticks", 0) + 1
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
            p["liq_now"] = liq
            exit_eff = px * (1 - side_cost(liq, size_usd))
            net = exit_eff / p["entry_eff"] - 1
            xq = self.exec_floor(p, sell_fn, exec_cache)
            if xq is not None:
                ctx = p.setdefault("ctx", {})
                ctx["xq"], ctx["xm"] = round(xq, 4), round(net, 4)
                if xq < net - 0.05:
                    logger.warning(
                        f"[exec] {p['symbol']} sell quote {xq:+.1%} vs model "
                        f"{net:+.1%}; recording the quote"
                    )
                net = min(net, xq)
            self._close(p, s, reason, px, net, now)
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
            syms = st.setdefault("syms", {})
            sym = (p.get("symbol") or p["mint"]).upper()
            syms[sym] = syms.get(sym, 0) + 1
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
            fams = Counter(s["family"] for s in self.strategies)
            if fams[child["family"]] >= MAX_FAMILY_SHARE * POP:
                child = random_strategy(
                    self.rng, min(SEARCH_FAMILIES, key=lambda f: fams[f])
                )
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
            f"tokens {len(st.get('mints') or {})} "
            f"tickers {len(st.get('syms') or {})} | {describe(s)}"
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
        farm = {sym for sym, mints in self.symbol_mints.items() if len(mints) > 1}
        split = ledger_split(DATA / "trades.csv", farm, time.time() - 48 * 3600)
        if split:
            lines.append(
                "\nLedger, last 48 h (relaunch tickers = one ticker on 2+ mints):"
            )
            lines += split
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
                "mint_created",
                "mint_liq",
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
            for b in BOOLS:
                s.setdefault(b, False)
            repair(s)
        for tag, specs in INJECTED_SEEDS.items():
            if tag not in self.injected:
                self._inject(tag, specs)
        now = time.time()
        self.cooldown = {k: v for k, v in self.cooldown.items() if v > now}
        self.symbol_mints = state.get("symbol_mints") or {}
        self.mint_created = state.get("mint_created") or {}
        self.mint_liq = state.get("mint_liq") or {}
        self.traded_mints = set()
        self.load_traded_mints(DATA / "trades.csv")
        self.backfill_symbols(DATA / "trades.csv")
        self.backfill_tickers(DATA / "trades.csv")
        self.candidates = {
            k: v
            for k, v in self.candidates.items()
            if k not in self.by_id() or is_candidate(self.by_id()[k]["stats"])
        }
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


SNAP_KEEP_DAYS = 14


def record_snapshot(snap: dict[str, dict], now: float) -> None:
    """Append one discovery snapshot to snapshots/YYYY-MM-DD.jsonl.gz for backtests.

    One line per snapshot: {"t": unix s, "pools": [compact pool records]}. Files
    older than SNAP_KEEP_DAYS are deleted. Failures are logged and ignored; the
    recorder must never stop the paper engine.
    """
    try:
        d = DATA / "snapshots"
        d.mkdir(exist_ok=True)
        pools = [
            {
                "mint": r["mint"],
                "pool": r["pool"],
                "sym": r["symbol"],
                "dex": r["dex"],
                "sol": r["quote_is_sol"],
                "px": r["price"],
                "liq": round(r["liq"]),
                "mcap": round(r["mcap"]),
                "age": round(r["age_h"], 2),
                "pc": {k: round(v, 5) for k, v in r["pc"].items()},
                "tx": r["tx"],
                "vol": {k: round(v) for k, v in r["vol"].items()},
                "rk": r["ranks"],
            }
            for r in snap.values()
        ]
        day = datetime.fromtimestamp(now, UTC).strftime("%Y-%m-%d")
        with gzip.open(d / f"{day}.jsonl.gz", "at") as f:
            f.write(
                json.dumps({"t": round(now), "pools": pools}, separators=(",", ":"))
                + "\n"
            )
        cutoff = now - SNAP_KEEP_DAYS * 86400
        for old in d.glob("*.jsonl.gz"):
            if old.stat().st_mtime < cutoff:
                old.unlink()
    except (OSError, KeyError, TypeError, ValueError) as e:
        logger.warning(f"snapshot record failed: {e}")


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

    grads = GradSource()
    next_disc = next_quote = next_save = next_sol = 0.0
    while not stop["flag"]:
        t = time.time()
        if t >= next_sol:
            px = feeds.sol_usd()
            if px:
                eng.sol_usd = px
            next_sol = t + 600
        if t >= next_disc:
            snap = feeds.discover(PUMPSWAP_PAGES, grads.refresh(t))
            if snap:
                record_snapshot(snap, t)
                eng.on_snapshot(snap, feeds.quotes)
            else:
                logger.warning("empty discovery snapshot")
            next_disc = t + DISCOVER_S
        if eng.positions and t >= next_quote:
            eng.on_quotes(
                feeds.quotes({p["pool"]: p["mint"] for p in eng.positions}),
                feeds.jup_sell_lamports,
            )
            next_quote = time.time() + QUOTE_S
        if t - eng.last_evolve >= EVOLVE_H * 3600:
            eng.evolve()
        if t >= next_save:
            eng.save()
            logger.info(
                f"[status] gen {eng.generation} open {len(eng.positions)} "
                f"closed {eng.totals['closed']} candidates {len(eng.candidates)} "
                f"funnel {getattr(eng, 'funnel', {})} "
                f"grads {grads.mints_seen} mints -> {len(grads.pools)} pools"
            )
            next_save = t + 60
        time.sleep(0.5)
    eng.save()
    logger.info("stopped; state saved")


if __name__ == "__main__":
    main()
