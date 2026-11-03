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
    crossover,
    describe,
    entry_signal,
    exit_reason,
    mutate,
    random_strategy,
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
    }


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
    return (
        st["sum"] / st["n"] > 0.005
        and profit_factor(st) > 1.25
        and sum(r[:h]) > 0
        and sum(r[h:]) > 0
    )


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

    # ---------- population ----------
    def seed(self) -> None:
        for spec in SEEDS:
            s = random_strategy(self.rng, spec["family"])
            s.update(spec)
            self.strategies.append(self._wrap(s))
        fams = list(FAMILIES)
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
                        "h24": rec["pc"]["h24"],
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
                drained = fq["liq"] < 0.3 * p["liq0"]
                spiked = price > 3 * p["last"] and fq["liq"] < 0.5 * p["liq0"]
                if drained or spiked:
                    # Pool pulled: a drained pool's quoted price is not sellable.
                    reason, px = "rug", min(price, p["last"]) * 0.3
                else:
                    p["last"] = price
                    p["peak"] = max(p["peak"], price)
                    reason = exit_reason(s, p, price, fq, now)
                    px = price
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
        drop = {s["id"] for s in cut + idle}
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
        # Keep every family in the search, so an early lucky streak in one
        # family cannot crowd out the others before they have been tested.
        for fam in FAMILIES:
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
            f"({len(cut)} weak, {len(idle)} idle); best: "
            f"{self._line(ranked[0]) if ranked else 'n/a'}"
        )

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
            f"total {st['sum'] * SIZE_SOL:+.4f} SOL | {describe(s)}"
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
        now = time.time()
        self.cooldown = {k: v for k, v in self.cooldown.items() if v > now}
        return True


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
