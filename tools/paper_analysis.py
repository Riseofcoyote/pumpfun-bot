"""Compact offline analysis of the trending and devwatch paper results.

Usage: python tools/paper_analysis.py <trending trades.csv> <devwatch results.csv>
Either path may be missing. Prints small tables only, so a workflow log stays
readable from a phone or an agent that cannot download artifacts.
"""

import csv
import json
import statistics as st
import sys
from collections import defaultdict
from datetime import datetime


def stats(rets: list[float]) -> str:
    """One line: n, mean, median, profit factor, win rate, halves, mean without top 3."""
    n = len(rets)
    if not n:
        return "n=0"
    gains = sum(r for r in rets if r > 0)
    loss = -sum(r for r in rets if r < 0)
    pf = gains / loss if loss else float("inf")
    h = n // 2
    trim = sorted(rets)[:-3] if n > 6 else rets
    return (
        f"n={n:5d} avg {st.mean(rets):+7.2%} med {st.median(rets):+7.2%} "
        f"pf {pf:5.2f} win {sum(r > 0 for r in rets) / n:4.0%} "
        f"h1 {sum(rets[:h]):+6.2f} h2 {sum(rets[h:]):+6.2f} "
        f"avg-top3 {st.mean(trim):+7.2%}"
    )


def table(title: str, groups: dict, min_n: int = 1, top: int = 25) -> None:
    print(f"\n-- {title}")
    rows = [(k, v) for k, v in groups.items() if len(v) >= min_n]
    rows.sort(key=lambda kv: -st.mean(kv[1]))
    for k, v in rows[:top]:
        print(f"  {str(k)[:28]:28} {stats(v)}")
    if len(rows) > top:
        print(f"  ... {len(rows) - top} more")


def trending(path: str) -> None:
    try:
        rows = list(csv.DictReader(open(path, newline="")))
    except OSError as e:
        print(f"trending: {e}")
        return
    mints = defaultdict(set)
    for r in rows:
        mints[(r.get("symbol") or "").upper()].add(r.get("mint"))
    relaunch = {s for s, m in mints.items() if len(m) >= 2}
    print(f"relaunch tickers (one symbol on 2+ mints): {sorted(relaunch)}")
    trending_part(rows, "all")
    trending_part(
        [r for r in rows if (r.get("symbol") or "").upper() not in relaunch],
        "excluding relaunch tickers",
    )


def trending_part(rows: list[dict], label: str) -> None:
    trades = []
    for r in rows:
        if r.get("reason") == "retired":
            continue
        try:
            r["_ret"] = float(r["net_ret"])
            r["_t"] = datetime.strptime(r["time_utc"], "%Y-%m-%dT%H:%M:%SZ")
        except (KeyError, ValueError, TypeError):
            continue
        trades.append(r)
    print(f"\n=== TRENDING [{label}] {len(trades)} closed trades", end="")
    if not trades:
        print()
        return
    print(f" {trades[0]['time_utc']} .. {trades[-1]['time_utc']}")
    rets = [r["_ret"] for r in trades]
    print("  all", stats(rets))
    big = sorted(trades, key=lambda r: -abs(r["_ret"]))[:8]
    print("-- largest |net| trades (outlier check)")
    for r in big:
        print(
            f"  {r['time_utc']} {r['sid'][:14]:14} {r['symbol'][:10]:10} "
            f"{r['reason'][:12]:12} {r['_ret']:+.1%} held {r['held_min']}m"
        )
    print("-- last 8 trades")
    for r in trades[-8:]:
        print(
            f"  {r['time_utc']} {r['sid'][:8]:8} {r['family'][:13]:13} "
            f"{r['symbol'][:10]:10} {r['reason'][:12]:12} {r['_ret']:+.1%}"
        )
    by = defaultdict(lambda: defaultdict(list))
    for r in trades:
        by["family"][r["family"]].append(r["_ret"])
        by["reason"][r["reason"]].append(r["_ret"])
        by["sid"][f"{r['sid']} ({r['family']})"].append(r["_ret"])
        by["hour"][r["_t"].hour].append(r["_ret"])
        by["day"][r["_t"].strftime("%m-%d")].append(r["_ret"])
        by["symbol"][r["symbol"]].append(r["_ret"])
        try:
            ctx = json.loads(r.get("ctx") or "{}")
        except ValueError:
            ctx = {}
        h1, h6 = ctx.get("h1"), ctx.get("h6")
        if isinstance(h1, (int, float)) and isinstance(h6, (int, float)):
            calm = abs(h1) < 0.03 and abs(h6) < 0.06  # trending engine CALM_H1/H6
            by["calm"]["calm" if calm else "moving"].append(r["_ret"])
            if calm:
                by["calm_symbol"][r["symbol"]].append(r["_ret"])
                by["calm_day"][r["_t"].strftime("%m-%d")].append(r["_ret"])
                by["calm_reason"][r["reason"]].append(r["_ret"])
        for k, v in ctx.items():
            if isinstance(v, bool) or isinstance(v, str):
                by[f"ctx.{k}"][v].append(r["_ret"])
            elif isinstance(v, (int, float)):
                by[f"ctx.{k}"][v].append(r["_ret"])
    table("by family", by["family"])
    table("by exit reason", by["reason"])
    table("by strategy (n>=30)", by["sid"], 30, 20)
    table("by day", by["day"], 1, 40)
    table("by hour UTC", by["hour"], 1, 24)
    table("by symbol (n>=15), top", by["symbol"], 15, 12)
    table("calm entry (|h1|<3% and |h6|<6%) vs moving", by["calm"])
    table("calm entries by symbol", by["calm_symbol"], 1, 20)
    table("calm entries by day", by["calm_day"], 1, 20)
    table("calm entries by exit reason", by["calm_reason"])
    sym = by["symbol"]
    worst = sorted(
        ((k, v) for k, v in sym.items() if len(v) >= 15), key=lambda kv: sum(kv[1])
    )[:6]
    print("-- worst symbols by total")
    for k, v in worst:
        print(f"  {k[:28]:28} {stats(v)}")
    ctx_tables(by)
    young = [
        r for r in trades if 4 <= (_ctx(r).get("age_h") or -1) <= 12
    ]
    if young:
        yb = defaultdict(lambda: defaultdict(list))
        for r in young:
            yb["family"][r["family"]].append(r["_ret"])
            yb["reason"][r["reason"]].append(r["_ret"])
            yb["day"][r["_t"].strftime("%m-%d")].append(r["_ret"])
            yb["symbol"][r["symbol"]].append(r["_ret"])
            for k, v in _ctx(r).items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    yb[f"ctx.{k}"][v].append(r["_ret"])
        print(f"\n=== YOUNG POOLS (ctx.age_h 4-12) {len(young)} trades")
        print("  all", stats([r["_ret"] for r in young]))
        print(f"  distinct symbols {len(yb['symbol'])}")
        table("young by family", yb["family"])
        table("young by exit reason", yb["reason"])
        table("young by day", yb["day"], 1, 20)
        table("young by symbol (n>=5)", yb["symbol"], 5, 15)
        ctx_tables(yb)


def _ctx(r: dict) -> dict:
    try:
        c = json.loads(r.get("ctx") or "{}")
    except ValueError:
        return {}
    return c if isinstance(c, dict) else {}


def ctx_tables(by: dict) -> None:
    """Quartile tables for every numeric ctx.* group in ``by``."""
    for key in sorted(k for k in by if k.startswith("ctx.")):
        vals = by[key]
        nums = [
            x for x in vals if isinstance(x, (int, float)) and not isinstance(x, bool)
        ]
        if len(nums) > 8:
            pairs = sorted((x, r) for x in nums for r in vals[x])
            q = len(pairs) // 4 or 1
            print(f"\n-- {key} quartiles")
            for i in range(4):
                part = pairs[i * q : (i + 1) * q if i < 3 else None]
                if part:
                    lo, hi = part[0][0], part[-1][0]
                    print(f"  {lo:10.4g}..{hi:<10.4g} {stats([r for _, r in part])}")
        else:
            table(key, vals, 10, 12)


QUOTE_FIX_UTC = "2026-11-09T12:40:00Z"


def suspect(r: dict) -> bool:
    """Mirror of devwatch's ``suspect``: a value we could not have sold at.

    Graduated paths before QUOTE_FIX_UTC were valued at Jupiter's mid price
    (36-160x prints), and an ungraduated curve frozen above 1.5x from minute 5
    to 60 was filled off a stale read. Keep in step with src/devwatch/engine.py;
    the workflow runs without the bot's dependencies, so it cannot import it.
    """
    if r.get("end") in ("unpriced", "lost"):
        return True
    grad = r.get("graduated") in ("True", "true", "1")
    if grad and r.get("time_utc", "") < QUOTE_FIX_UTC:
        return True
    try:
        m5, m15, m60 = (float(r[f"mult_m{m}"]) for m in (5, 15, 60))
    except (KeyError, TypeError, ValueError):
        return False
    if r.get("end"):
        return not grad and m5 > 1.5 and m5 == m15 == m60
    return not grad and m5 > 2 and m5 == m60


def devwatch(path: str) -> None:
    try:
        rows = list(csv.DictReader(open(path, newline="")))
    except OSError as e:
        print(f"devwatch: {e}")
        return
    print(f"\n=== DEVWATCH {len(rows)} results", end="")
    if not rows:
        print()
        return
    print(f" {rows[0]['time_utc']} .. {rows[-1]['time_utc']}")
    rules = [c[:-4] for c in rows[0] if c.endswith("_net")]
    kinds = defaultdict(list)
    for r in rows:
        kinds[r["kind"]].append(r)
    print("-- per kind: count, graduated, peak multiple median/p90, end status")
    for k, rs in sorted(kinds.items(), key=lambda kv: -len(kv[1])):
        peaks = sorted(float(r["peak_mult"] or 0) for r in rs)
        grad = sum(r.get("graduated") in ("1", "True", "true") for r in rs)
        ends = defaultdict(int)
        for r in rs:
            ends[r.get("end") or "-"] += 1
        p90 = peaks[int(len(peaks) * 0.9)] if peaks else 0
        print(
            f"  {k:22} n={len(rs):5d} grad {grad / len(rs):5.1%} "
            f"peak med {st.median(peaks) if peaks else 0:5.2f} p90 {p90:6.2f} "
            f"end {dict(ends)}"
        )
    clean = lambda r: (r.get("end") or "ok") == "ok" and not suspect(r)  # noqa: E731
    print(f"-- {sum(map(suspect, rows))} suspect rows dropped from the rule tables")
    for rule in rules:
        groups = {}
        for k, rs in kinds.items():
            vals = []
            for r in rs:
                if not clean(r):
                    continue
                v = r.get(f"{rule}_net")
                if v in ("", None):
                    continue
                try:
                    vals.append(float(v))
                except ValueError:
                    pass
            if vals:
                groups[k] = vals
        table(f"rule {rule} (end ok/blank; skip rows excluded)", groups, 1, 12)
        best = []
        for k, rs in kinds.items():
            if k == "control":
                continue
            for r in rs:
                try:
                    best.append((float(r.get(f"{rule}_net") or "x"), k, r))
                except ValueError:
                    pass
        best.sort(key=lambda x: -x[0])
        for v, k, r in best[:4]:
            print(
                f"    top {k[:10]:10} {r['symbol'][:10]:10} {v:+8.1%} peak "
                f"{r['peak_mult']} grad {r['graduated']} m60 {r['mult_m60']} "
                f"end {r.get('end') or '-'} why {r.get(f'{rule}_why')} {r['time_utc']}"
            )
    # Time halves for the main signal vs control under each rule.
    print("\n-- halves by kind (sum net, end=ok) per rule: first half / second half")
    for rule in rules:
        parts = []
        for k, rs in sorted(kinds.items()):
            vals = []
            for r in rs:
                v = r.get(f"{rule}_net")
                if clean(r) and v not in ("", None):
                    try:
                        vals.append(float(v))
                    except ValueError:
                        pass
            if len(vals) >= 20:
                h = len(vals) // 2
                parts.append(f"{k} {st.mean(vals[:h]):+.1%}/{st.mean(vals[h:]):+.1%}")
        print(f"  {rule:10} " + "; ".join(parts))
    late_by_entry(rows, clean)
    days = defaultdict(lambda: defaultdict(int))
    for r in rows:
        days[r["time_utc"][:10]][r["kind"]] += 1
    print("\n-- results per day by kind")
    for d in sorted(days)[-10:]:
        print(f"  {d} {dict(days[d])}")


LATE_BANDS = ((0.6, 0.9), (0.9, 1.2), (1.2, 2.0), (2.0, 4.0), (4.0, 1e9))


def late_by_entry(rows: list[dict], clean) -> None:  # noqa: ANN001
    """Late-entry rules split by the launch's value multiple when the entry fills.

    A d5 rule enters at roughly ``mult_m5`` and a d15 rule at ``mult_m15``, so a
    band compares signal and control launches in the same state at entry.
    """
    print("\n-- late entries by value multiple at entry (kind: stats)")
    for rule, col in (("d5_trail", "mult_m5"), ("d5_hold", "mult_m5"),
                      ("d15_trail", "mult_m15")):
        for lo, hi in LATE_BANDS:
            groups = defaultdict(list)
            for r in rows:
                v = r.get(f"{rule}_net")
                if not clean(r) or v in ("", None) or r.get(f"{rule}_why") == "skip":
                    continue
                try:
                    m, net = float(r.get(col) or "x"), float(v)
                except ValueError:
                    continue
                if lo <= m < hi:
                    groups[r["kind"]].append(net)
            hi_s = "+" if hi > 1e8 else f"{hi:g}"
            table(f"{rule} entry x{lo:g}..{hi_s}", groups, 10, 6)


if __name__ == "__main__":
    args = sys.argv[1:] + ["", ""]
    if args[0]:
        trending(args[0])
    if args[1]:
        devwatch(args[1])
