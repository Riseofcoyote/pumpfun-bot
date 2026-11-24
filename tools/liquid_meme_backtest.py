"""Walk-forward backtest of short-hold strategies on liquid, established Solana coins.

Every pump.fun paper tester so far lost after costs on young coins, mostly to
rugs and a 4-10% round trip. This asks whether the same short holds work on
coins that cannot rug overnight: pools at least 7 days old with $300k+
liquidity, where a 0.2 SOL round trip costs well under 1%.

Five strategy families on 5-minute candles (signal at a bar's close, fill at
the next bar's open, stop checked before target inside a bar):
  dev        close d under its n-bar average -> sell at that average
  zscore     close z std-devs under its 20-bar mean (optional 200-bar uptrend)
  rsi2       2-bar RSI under a threshold (optional 100-bar uptrend), exit over SMA5
  capit      one bar down r on k x average volume -> fixed target/stop
  breakout   close over the n-bar high on k x volume -> fixed target/stop (contrast)

Parameters are picked on the first 60% of the window (all coins pooled) and
scored on the last 40%. Three cost passes per side: none (is there
any edge at all?), a flat 0.3% (pool fee plus network fee on a deep pool), and
the dex's fee+slippage plus 0.1% latency.

Run from the repo root: python -u tools/liquid_meme_backtest.py
"""

from __future__ import annotations

import itertools
import os
import statistics as st
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trending import feeds  # noqa: E402

DAYS = float(os.environ.get("BT_DAYS", "14"))
N_POOLS = int(os.environ.get("BT_POOLS", "40"))
# Candle size: "minute:5" (default), "minute:15", "hour:1". Bar counts in the grids scale with it.
TF, AGG = os.environ.get("BT_TF", "minute:5").split(":")
SIDE_COST = {"pumpswap": 0.005, "raydium": 0.004, "raydium-clmm": 0.004, "orca": 0.004,
             "meteora": 0.006, "meteora-damm-v2": 0.006}
LATENCY = 0.001
SKIP = {"SOL", "WSOL", "USDC", "USDT", "USD1", "PYUSD", "USDS", "JITOSOL", "MSOL", "JUPSOL",
        "BSOL", "INF", "CBBTC", "WBTC", "WETH", "ZBTC", "EURC", "FDUSD", "USDG"}


def universe(n: int) -> list[dict]:
    pools = dict(feeds.discover())
    for sort in ("h24_volume_usd_desc", "h24_tx_count_desc"):
        for page in range(1, 11):
            doc = feeds.get_json(f"{feeds.GT}/networks/solana/pools?page={page}&sort={sort}",
                                 feeds.GT_GATE)
            for p in (doc or {}).get("data") or []:
                rec = feeds._parse_gt_pool(p)
                if rec:
                    pools.setdefault(rec["pool"], rec)
    best: dict[str, dict] = {}
    for r in pools.values():
        if r["age_h"] < 168 or r["liq"] < 300_000 or r["symbol"].upper() in SKIP:
            continue
        if r["symbol"].endswith("x") and r["symbol"][:-1].isupper():  # tokenized stocks
            continue
        if r["symbol"].upper().startswith("USD"):
            continue
        prev = best.get(r["mint"])
        if prev is None or r["vol"]["h24"] > prev["vol"]["h24"]:
            best[r["mint"]] = r
    return sorted(best.values(), key=lambda r: -r["vol"]["h24"])[:n]


def candles(pool: str, days: float) -> list[tuple]:
    out: dict[float, tuple] = {}
    before = int(time.time())
    stop = before - days * 86400
    for _ in range(12):
        doc = feeds.get_json(
            f"{feeds.GT}/networks/solana/pools/{pool}/ohlcv/{TF}?aggregate={AGG}&limit=1000"
            f"&currency=usd&before_timestamp={before}", feeds.GT_GATE)
        rows = (((doc or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
        if not rows:
            break
        for t, o, h, lo, c, v in rows:
            if c and o:
                out[float(t)] = (float(t), float(o), float(h), float(lo), float(c), float(v or 0))
        oldest = min(float(r[0]) for r in rows)
        if oldest <= stop or len(rows) < 900:
            break
        before = int(oldest)
    return sorted(r for r in out.values() if r[0] >= stop)


# ---------- indicators ----------

def sma(x: list[float], n: int) -> list[float | None]:
    out: list[float | None] = [None] * len(x)
    s = 0.0
    for i, v in enumerate(x):
        s += v
        if i >= n:
            s -= x[i - n]
        if i >= n - 1:
            out[i] = s / n
    return out


def rsi2(c: list[float]) -> list[float | None]:
    out: list[float | None] = [None] * len(c)
    up = dn = 0.0
    a = 0.5
    for i in range(1, len(c)):
        d = c[i] - c[i - 1]
        up = (1 - a) * up + a * max(d, 0.0)
        dn = (1 - a) * dn + a * max(-d, 0.0)
        if i >= 3:
            out[i] = 100.0 if dn == 0 else 100 - 100 / (1 + up / dn)
    return out


class Coin:
    def __init__(self, sym: str, cost: float, c: list[tuple]) -> None:
        self.sym, self.cost, self.c = sym, cost, c
        self.t = [r[0] for r in c]
        self.o = [r[1] for r in c]
        self.h = [r[2] for r in c]
        self.lo = [r[3] for r in c]
        self.cl = [r[4] for r in c]
        self.v = [r[5] for r in c]
        self._sma: dict[int, list] = {}
        self._std20: list | None = None
        self.r2 = rsi2(self.cl)
        self.v20 = sma(self.v, 20)

    def sma(self, n: int) -> list:
        if n not in self._sma:
            self._sma[n] = sma(self.cl, n)
        return self._sma[n]

    def std20(self) -> list:
        if self._std20 is None:
            out: list[float | None] = [None] * len(self.cl)
            for i in range(19, len(self.cl)):
                out[i] = st.pstdev(self.cl[i - 19:i + 1])
            self._std20 = out
        return self._std20


def walk(cn: Coin, i: int, tp: float | None, sl: float, hold: int,
         exit_fn=None) -> tuple[float, int]:
    """Enter at bar i's open; return (gross return, exit bar)."""
    e = cn.o[i]
    stop = e * (1 - sl)
    last = min(len(cn.cl) - 1, i + hold)
    for j in range(i, last + 1):
        if cn.lo[j] <= stop:
            return stop / e - 1, j
        if tp is not None and cn.h[j] >= e * (1 + tp) and j > i:
            return tp, j
        if exit_fn is not None and exit_fn(j) and j + 1 < len(cn.o):
            return cn.o[j + 1] / e - 1, j + 1
    return cn.cl[last] / e - 1, last


def trades(cn: Coin, fam: str, p: tuple, t0: float, t1: float, extra: float) -> list[tuple]:
    out = []
    cost = cn.cost if extra is None else extra
    n = len(cn.cl)
    i = 200
    while i < n - 2:
        if not (t0 <= cn.t[i] < t1):
            i += 1
            continue
        sig = False
        tp: float | None = None
        exit_fn = None
        if fam == "dev":
            look, d, sl, hold = p
            m = cn.sma(look)[i]
            if m and cn.cl[i] < m * (1 - d):
                sig, tp = True, max(m / cn.cl[i] - 1, 0.004)
        elif fam == "zscore":
            zk, trend, sl, hold = p
            m, s = cn.sma(20)[i], cn.std20()[i]
            if m and s and (cn.cl[i] - m) / s < -zk and (not trend or cn.cl[i] > cn.sma(200)[i]):
                sig = True
                mm = cn.sma(20)
                exit_fn = lambda j, mm=mm: mm[j] is not None and cn.cl[j] >= mm[j]  # noqa: E731
        elif fam == "rsi2":
            th, trend, sl, hold = p
            r = cn.r2[i]
            if r is not None and r < th and (not trend or cn.cl[i] > cn.sma(100)[i]):
                sig = True
                m5 = cn.sma(5)
                exit_fn = lambda j, m5=m5: m5[j] is not None and cn.cl[j] > m5[j]  # noqa: E731
        elif fam == "capit":
            r, k, tp_, sl, hold = p
            va = cn.v20[i - 1]
            if va and cn.cl[i] < cn.o[i] * (1 - r) and cn.v[i] > k * va:
                sig, tp = True, tp_
        elif fam == "breakout":
            look, k, tp_, sl, hold = p
            va = cn.v20[i - 1]
            if va and cn.cl[i] > max(cn.h[i - look:i]) and cn.v[i] > k * va:
                sig, tp = True, tp_
        if not sig:
            i += 1
            continue
        g, j = walk(cn, i + 1, tp, p[-2], p[-1], exit_fn)
        net = (1 + g) * (1 - cost) ** 2 - 1
        out.append((cn.t[i + 1], cn.sym, net))
        i = j + 1
    return out


GRIDS = {
    "dev": list(itertools.product((12, 24, 48), (0.02, 0.03, 0.05, 0.08), (0.03, 0.06, 0.10), (12, 36))),
    "zscore": list(itertools.product((2.0, 2.5, 3.0), (0, 1), (0.05, 0.10), (12, 36))),
    "rsi2": list(itertools.product((5, 10, 20), (0, 1), (0.05, 0.10), (12, 24))),
    "capit": list(itertools.product((0.03, 0.05, 0.08), (2, 4), (0.02, 0.04, 0.08), (0.05, 0.10), (12, 36))),
    "breakout": list(itertools.product((12, 48), (2, 4), (0.03, 0.06, 0.12), (0.03, 0.06), (12, 36))),
}


def stats(tr: list[tuple], hours: float) -> str:
    if not tr:
        return "no trades"
    x = [r[2] for r in tr]
    gains = sum(v for v in x if v > 0)
    loss = -sum(v for v in x if v < 0)
    by_coin: dict[str, float] = {}
    by_day: dict[int, float] = {}
    for t, s, v in tr:
        by_coin[s] = by_coin.get(s, 0) + v
        by_day[int(t // 86400)] = by_day.get(int(t // 86400), 0) + v
    eq = peak = dd = 0.0
    for v in sorted(tr):
        eq += v[2]
        peak = max(peak, eq)
        dd = min(dd, eq - peak)
    pf = gains / loss if loss else float("inf")
    return (f"n {len(x):4d} ({len(x) / hours:.2f}/h) win {sum(v > 0 for v in x) / len(x):4.0%} "
            f"mean {st.mean(x):+6.2%} med {st.median(x):+6.2%} PF {pf:4.2f} worst {min(x):+6.1%} "
            f"maxDD {dd:+6.1%} coins+ {sum(v > 0 for v in by_coin.values())}/{len(by_coin)} "
            f"days+ {sum(v > 0 for v in by_day.values())}/{len(by_day)}")


def main() -> None:
    uni = universe(N_POOLS)
    print(f"{len(uni)} pools: " + ", ".join(f"{r['symbol']}({r['dex']},${r['liq'] / 1e6:.1f}M)" for r in uni))
    coins: list[Coin] = []
    for r in uni:
        c = candles(r["pool"], DAYS)
        if len(c) < 400:
            print(f"  {r['symbol']}: only {len(c)} candles, skipped")
            continue
        rets = [c[i][4] / c[i - 1][4] - 1 for i in range(1, len(c))]
        sd = st.pstdev(rets)
        cost = SIDE_COST.get(r["dex"], 0.006) + LATENCY
        print(f"  {r['symbol']:10} {len(c):5d} candles  bar sd {sd:.2%}  cost/side {cost:.2%}")
        if sd < 0.003 and TF == "minute" and AGG == "5":
            print("    too calm to cover costs, skipped")
            continue
        coins.append(Coin(r["symbol"], cost, c))
    t_lo = min(cn.t[200] for cn in coins)
    t_hi = max(cn.t[-1] for cn in coins)
    cut = t_lo + 0.6 * (t_hi - t_lo)
    tr_h = (cut - t_lo) / 3600
    te_h = (t_hi - cut) / 3600
    print(f"\n{len(coins)} coins; train {tr_h:.0f} h, test {te_h:.0f} h (all coins together)")
    for extra in (0.0, 0.003, None):
        label = "the dex's fee+slippage+latency" if extra is None else f"{extra:.1%}"
        print(f"\n########## cost per side: {label}")
        for fam, grid in GRIDS.items():
            scored = []
            for p in grid:
                tr = [x for cn in coins for x in trades(cn, fam, p, t_lo, cut, extra)]
                if len(tr) >= 40:
                    scored.append((st.mean(r[2] for r in tr), p, tr))
            scored.sort(key=lambda s: -s[0])
            pos = sum(s[0] > 0 for s in scored)
            print(f"\n== {fam}: {pos}/{len(scored)} train configs positive")
            for mean, p, tr in scored[:3]:
                te = [x for cn in coins for x in trades(cn, fam, p, cut, t_hi + 1, extra)]
                print(f"  {p}\n     TRAIN {stats(tr, tr_h)}\n     TEST  {stats(te, te_h)}")
            all_te = []
            for _m, p, _tr in scored:
                te = [x for cn in coins for x in trades(cn, fam, p, cut, t_hi + 1, extra)]
                if te:
                    all_te.append(st.mean(r[2] for r in te))
            if all_te:
                print(f"  every config on TEST: {sum(v > 0 for v in all_te)}/{len(all_te)} positive, "
                      f"median config mean {st.median(all_te):+.2%}")


if __name__ == "__main__":
    main()
