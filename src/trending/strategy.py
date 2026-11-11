"""Strategy genomes for the trending paper engine: entry rules, exits, mutation.

A strategy is a flat dict of parameters (JSON-serializable, so the population
survives restarts). Fractions throughout: 0.05 means 5%.
"""

from __future__ import annotations

import random
import string

FAMILIES = ("momentum", "dip", "surge", "fresh_trend", "volume_leader")
# Families new random strategies and the per-family quota draw from. fresh_trend
# (buying a pool as it enters the 5-minute list) lost after costs under every
# engine version and on the average token, so it gets no new seeds or quota;
# live members stay until evolution cuts them, and an elite one may still breed.
SEARCH_FAMILIES = ("momentum", "dip", "surge", "volume_leader")
SOURCES = ("any", "trend5m", "trend1h", "volume")
# Pools younger than this are fresh launches, not the established trending and
# top-volume tokens this engine is for; no strategy may buy them.
MIN_POOL_AGE_H = 6.0
# Hard entry limits no genome can relax. Pools under $50k liquidity and buys
# into a 5-minute candle already up 10% lost after costs in every ledger so far.
MIN_LIQ_USD = 50_000.0
MAX_ENTRY_M5 = 0.10
# Entries into a pool already up more than 200% over 24 hours lost after costs in
# every family, and held all but a handful of the rugs (farm tokens pulled at
# the top of their cycle).
MAX_ENTRY_H24 = 2.0
# Entries into a pool already up more than 15% over the last hour lost about 9%
# net (7% before costs) in every family and on every day, across 30 tokens.
MAX_ENTRY_H1 = 0.15

# name -> (low, high, kind). kind "f" float, "i" int, "log" float sampled in log space.
RANGES: dict[str, tuple[float, float, str]] = {
    "min_liq": (50_000, 500_000, "log"),
    "max_mcap": (200_000, 200_000_000, "log"),
    "min_age_h": (6, 72, "f"),
    "max_age_h": (10, 5_000, "log"),
    "max_rank": (8, 60, "i"),
    "m5_min": (-0.10, 0.05, "f"),
    "m5_max": (0.02, 0.10, "f"),
    "h1_min": (-0.30, 0.10, "f"),
    "h1_max": (0.05, MAX_ENTRY_H1, "log"),
    "h6_min": (-0.5, 2.0, "f"),
    "h24_min": (-0.9, 1.5, "f"),
    "h24_max": (0.2, MAX_ENTRY_H24, "log"),
    "buy_ratio_min": (0.6, 1.8, "f"),
    "min_m5_vol": (500, 60_000, "log"),
    "min_m5_buyers": (0, 60, "i"),
    "surge_k": (1.2, 5.0, "f"),
    # Skip pools ranked above this on the 5-minute trending list (1 = off):
    # the hottest few were the worst entries, ranks 20-40 the best.
    "min_t5_rank": (1, 25, "i"),
    "tp": (0.06, 1.0, "log"),
    "sl": (0.05, 0.50, "log"),
    "trail_act": (0.03, 0.5, "log"),
    "trail_dist": (0.015, 0.25, "log"),
    # Under ~20 minutes the typical move does not cover the ~2% round trip.
    "max_hold_min": (20, 360, "log"),
    "cooldown_min": (5, 120, "log"),
}
BOOLS = ("pump_only", "use_trail", "fade_exit")
LOOSE = {
    "min_liq": 50_000.0,
    "min_t5_rank": 1.0,
    "max_mcap": 200_000_000.0,
    "min_age_h": 6.0,
    "max_age_h": 5_000.0,
    "h6_min": -0.5,
    "h24_min": -0.9,
    "h24_max": MAX_ENTRY_H24,
    "min_m5_buyers": 0.0,
    "min_m5_vol": 500.0,
    "buy_ratio_min": 0.6,
    "h1_min": -0.30,
    "h1_max": MAX_ENTRY_H1,
}


def _sample(name: str, rng: random.Random) -> float:
    lo, hi, kind = RANGES[name]
    if kind == "log":
        lo2 = max(lo, 1e-6)
        import math

        return math.exp(rng.uniform(math.log(lo2), math.log(hi)))
    if kind == "i":
        return float(rng.randint(int(lo), int(hi)))
    return rng.uniform(lo, hi)


def new_id(rng: random.Random) -> str:
    return "".join(rng.choices(string.ascii_lowercase + string.digits, k=6))


def random_strategy(rng: random.Random, family: str | None = None) -> dict:
    s = {n: _sample(n, rng) for n in RANGES}
    for b in BOOLS:
        s[b] = rng.random() < 0.5
    # Flow-fade exits cut positions after a few minutes at about 0% gross, so
    # they mostly pay the round trip; keep the gene but start it rarer.
    s["fade_exit"] = rng.random() < 0.2
    s["family"] = family or rng.choice(SEARCH_FAMILIES)
    s["source"] = rng.choice(SOURCES)
    # Ten random filters stacked would almost never all pass, so each optional
    # one starts switched off (set to its permissive end) half the time.
    for name, loose in LOOSE.items():
        if rng.random() < 0.5:
            s[name] = loose
    if rng.random() < 0.5:
        s["source"] = "any"
    _repair(s)
    s["id"] = new_id(rng)
    s["parent"] = None
    return s


def _repair(s: dict) -> None:
    """Keep ranges coherent after sampling or mutation."""
    for n, (lo, hi, kind) in RANGES.items():
        s[n] = min(max(s[n], lo), hi)
        if kind == "i":
            s[n] = float(round(s[n]))
    s["m5_max"] = max(s["m5_max"], s["m5_min"] + 0.01)
    s["h1_max"] = max(s["h1_max"], s["h1_min"] + 0.05)
    s["max_age_h"] = max(s["max_age_h"], s["min_age_h"] + 1)
    s["h24_max"] = max(s["h24_max"], s["h24_min"] + 0.1)
    if s["family"] == "dip":
        # A dip buy needs an uptrend and a red 5 minutes.
        s["m5_max"] = min(s["m5_max"], 0.0)
        s["m5_min"] = min(s["m5_min"], -0.02)
        s["h1_min"] = max(s["h1_min"], 0.05)
        s["h1_max"] = max(s["h1_max"], s["h1_min"] + 0.05)
    if s["family"] == "fresh_trend" or s["source"] == "trend5m":
        # The rank cap has to leave a band above the hot-rank skip.
        s["max_rank"] = max(s["max_rank"], s["min_t5_rank"] + 5)
    if s["use_trail"] and s["trail_act"] > s["tp"]:
        s["trail_act"] = s["tp"] * 0.6


repair = _repair


def mutate(parent: dict, rng: random.Random, strength: float = 0.25) -> dict:
    """Copy of ``parent`` with a few parameters nudged; new id, ``parent`` set."""
    import math

    s = dict(parent)
    names = rng.sample(list(RANGES), k=rng.randint(1, 4))
    for n in names:
        lo, hi, kind = RANGES[n]
        if kind == "log":
            s[n] = math.exp(math.log(max(s[n], 1e-6)) + rng.gauss(0, strength * 1.5))
        else:
            s[n] = s[n] + rng.gauss(0, strength * (hi - lo))
    if rng.random() < 0.15:
        b = rng.choice(BOOLS)
        s[b] = not s[b]
    if rng.random() < 0.08:
        s["source"] = rng.choice(SOURCES)
    _repair(s)
    s["id"] = new_id(rng)
    s["parent"] = parent["id"]
    return s


def crossover(a: dict, b: dict, rng: random.Random) -> dict:
    s = {k: (a[k] if rng.random() < 0.5 else b[k]) for k in a}
    s["family"] = a["family"]
    _repair(s)
    s["id"] = new_id(rng)
    s["parent"] = f"{a['id']}x{b['id']}"
    return s


def is_pump(rec: dict) -> bool:
    return rec["mint"].endswith("pump") or "pump" in rec["dex"]


def entry_signal(s: dict, rec: dict, prev_ranks: dict[str, int] | None) -> bool:
    """True if ``s`` wants to buy the pool in snapshot record ``rec``.

    ``prev_ranks`` is the record's ranks from the previous snapshot, or None if
    the pool was not listed then.
    """
    if rec["age_h"] < MIN_POOL_AGE_H:
        return False
    if rec["liq"] < max(MIN_LIQ_USD, s["min_liq"]) or rec["mcap"] > s["max_mcap"]:
        return False
    if not (s["min_age_h"] <= rec["age_h"] <= s["max_age_h"]):
        return False
    if s["pump_only"] and not is_pump(rec):
        return False
    ranks = rec["ranks"]
    if s["source"] != "any":
        r = ranks.get(s["source"])
        if r is None or r > s["max_rank"]:
            return False
    pc, tx, vol = rec["pc"], rec["tx"]["m5"], rec["vol"]
    if pc["m5"] > MAX_ENTRY_M5 or pc["h24"] > MAX_ENTRY_H24 or pc["h1"] > MAX_ENTRY_H1:
        return False
    t5 = ranks.get("trend5m")
    if t5 is not None and t5 < s.get("min_t5_rank", 1.0):
        return False
    ratio = tx["buys"] / max(tx["sells"], 1)
    if ratio < s["buy_ratio_min"] or vol["m5"] < s["min_m5_vol"]:
        return False
    if tx["buyers"] < s["min_m5_buyers"]:
        return False
    if not s["h24_min"] <= pc["h24"] <= s.get("h24_max", MAX_ENTRY_H24):
        return False
    if pc.get("h6", 0.0) < s.get("h6_min", -0.5):
        return False
    fam = s["family"]
    in_m5 = s["m5_min"] <= pc["m5"] <= s["m5_max"]
    in_h1 = s["h1_min"] <= pc["h1"] <= s["h1_max"]
    if fam in ("momentum", "dip"):
        return in_m5 and in_h1
    if fam == "surge":
        base = vol["h1"] / 12 if vol["h1"] > 0 else 0
        return (
            base > 0
            and vol["m5"] >= s["surge_k"] * base
            and pc["m5"] >= s["m5_min"]
            and in_h1
        )
    if fam == "fresh_trend":
        r = ranks.get("trend5m")
        if r is None or r > s["max_rank"]:
            return False
        was = (prev_ranks or {}).get("trend5m")
        newly = was is None or was > s["max_rank"] or was - r >= 5
        return newly and pc["m5"] >= s["m5_min"] and in_h1
    if fam == "volume_leader":
        r = ranks.get("volume")
        return r is not None and r <= s["max_rank"] and in_m5 and in_h1
    return False


def exit_reason(
    s: dict, pos: dict, price: float, flow: dict | None, now: float
) -> str | None:
    """Why ``pos`` should close at ``price`` now, or None to hold.

    ``pos`` carries raw (pre-cost) ``entry`` and ``peak`` prices and ``t0``.
    """
    ret = price / pos["entry"] - 1
    peak_ret = pos["peak"] / pos["entry"] - 1
    if ret >= s["tp"]:
        return "take_profit"
    if ret <= -s["sl"]:
        return "stop_loss"
    if (
        s["use_trail"]
        and peak_ret >= s["trail_act"]
        and price <= pos["peak"] * (1 - s["trail_dist"])
    ):
        return "trailing"
    held_min = (now - pos["t0"]) / 60
    if held_min >= s["max_hold_min"]:
        return "max_hold"
    if (
        s["fade_exit"]
        and flow
        and held_min >= 10
        and flow["sells_m5"] > flow["buys_m5"] * 1.3
        and flow["sells_m5"] >= 10
    ):
        return "flow_fade"
    return None


def describe(s: dict) -> str:
    parts = [
        f"{s['family']}/{s['source']}",
        f"liq>{s['min_liq'] / 1e3:.0f}k mcap<{s['max_mcap'] / 1e6:.1f}M",
        f"age {s['min_age_h']:.0f}-{s['max_age_h']:.0f}h",
        f"m5 {s['m5_min']:+.0%}..{s['m5_max']:+.0%} h1 {s['h1_min']:+.0%}..{s['h1_max']:+.0%}",
        f"h6>{s.get('h6_min', -0.5):+.0%} h24 {s['h24_min']:+.0%}..{s.get('h24_max', MAX_ENTRY_H24):+.0%}",
        f"b/s>{s['buy_ratio_min']:.2f} vol5m>{s['min_m5_vol'] / 1e3:.1f}k",
        f"TP {s['tp']:.0%} SL {s['sl']:.0%}",
    ]
    if s["family"] == "surge":
        parts.append(f"surge x{s['surge_k']:.1f}")
    if s["source"] != "any" or s["family"] in ("fresh_trend", "volume_leader"):
        parts.append(f"rank<={s['max_rank']:.0f}")
    if s.get("min_t5_rank", 1.0) > 1:
        parts.append(f"t5rank>={s['min_t5_rank']:.0f}")
    if s["use_trail"]:
        parts.append(f"trail {s['trail_act']:.0%}/{s['trail_dist']:.0%}")
    parts.append(f"hold {s['max_hold_min']:.0f}m")
    if s["fade_exit"]:
        parts.append("fade-exit")
    if s["pump_only"]:
        parts.append("pump-only")
    return " | ".join(parts)
