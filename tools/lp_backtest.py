"""Paper backtest: provide liquidity on fresh PumpSwap graduates, and when to pull it out.

An LP earns the pool's LP fee on every swap and loses when the coin dumps (the
pool sells its SOL into the falling coin). This replays real pools candle by
candle and asks which exit rule and which deposit size leave more SOL than was
put in.

Pool model: pump.fun burns a graduate's LP tokens, so its reserves change only
by swaps and x*y = k stays put. k comes from the pool's reserves and price now,
which gives the SOL reserve at any past price: y = sqrt(k * p). The deposit
buys half its SOL in tokens first (paying the pool's full fee plus impact),
adds both sides, earns lp_fee * volume * share each 5-minute candle, and on
exit withdraws its share and sells the tokens back. Each trigger is read at a
candle's close and filled at the next candle's close. Four transactions at
0.0002 SOL each. LP fee tiers come from the PumpSwap fee config on chain.

Pools: a random sample of real graduations (tools/data/lp_grads.csv, epoch and
mint, from devwatch's graduation log), dead coins included, so the sample is
not just the survivors. Pools are split by graduation time: rules are ranked on
the older 60% and scored on the newer 40%.

Run from the repo root: python -u tools/lp_backtest.py
"""

from __future__ import annotations

import base64
import csv
import itertools
import json
import os
import random
import statistics as st
import struct
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trending import feeds  # noqa: E402

GRADS = Path(__file__).with_name("data") / "lp_grads.csv"
N_POOLS = int(os.environ.get("LP_POOLS", "400"))
WINDOW_H = 48
TX_SOL = 0.0002
SIZES = (0.1, 0.25, 0.5, 1.0, 2.0, 5.0)
PUMP_AMM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
FEE_PROGRAM = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ"
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"
SOL_USDC_POOL = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2"  # Raydium SOL/USDC
SUPPLY = 1e9  # pump.fun coins: 1B tokens


# ---------------- fee tiers ----------------

def fee_tiers() -> list[tuple[int, int, int, int]]:
    """[(mcap_lamports_threshold, lp_bps, protocol_bps, creator_bps)] ascending."""
    from solders.pubkey import Pubkey

    pda, _ = Pubkey.find_program_address(
        [b"fee_config", bytes(Pubkey.from_string(PUMP_AMM))], Pubkey.from_string(FEE_PROGRAM))
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                       "params": [str(pda), {"encoding": "base64"}]}).encode()
    req = urllib.request.Request(PUBLIC_RPC, body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:  # noqa: S310
        v = json.loads(r.read())["result"]["value"]
    raw = base64.b64decode(v["data"][0])
    off = 8 + 1 + 32 + 24  # discriminator, bump, admin, flat_fees
    (n,) = struct.unpack_from("<I", raw, off)
    off += 4
    tiers = []
    for _ in range(n):
        lo, hi = struct.unpack_from("<QQ", raw, off)
        lp, pr, cr = struct.unpack_from("<QQQ", raw, off + 16)
        tiers.append((lo + (hi << 64), lp, pr, cr))
        off += 40
    return sorted(tiers)


def fees_at(tiers: list, price_sol: float) -> tuple[float, float]:
    """(lp fee, total fee) as fractions for a pool at this SOL price per token."""
    mcap = price_sol * SUPPLY * 1e9
    pick = tiers[0]
    for t in tiers:
        if mcap >= t[0]:
            pick = t
    lp, pr, cr = pick[1:]
    return lp / 1e4, (lp + pr + cr) / 1e4


# ---------------- data ----------------

def pumpswap_pool(mint: str) -> dict | None:
    doc = feeds.get_json(f"{feeds.GT}/networks/solana/tokens/{mint}/pools?page=1", feeds.GT_GATE, feeds.GT_HDR)
    best = None
    for p in (doc or {}).get("data") or []:
        a = p.get("attributes") or {}
        rel = p.get("relationships") or {}
        dex = ((rel.get("dex") or {}).get("data") or {}).get("id", "")
        quote = ((rel.get("quote_token") or {}).get("data") or {}).get("id", "").split("_", 1)[-1]
        base = ((rel.get("base_token") or {}).get("data") or {}).get("id", "").split("_", 1)[-1]
        if "pump" not in dex or quote != feeds.SOL_MINT or base != mint:
            continue
        rec = {"pool": a.get("address"), "sym": (a.get("name") or "?").split(" / ")[0][:12],
               "price_usd": float(a.get("base_token_price_usd") or 0),
               "quote_usd": float(a.get("quote_token_price_usd") or 0),
               "liq": float(a.get("reserve_in_usd") or 0)}
        if best is None or rec["liq"] > best["liq"]:
            best = rec
    return best


def ohlcv(pool: str, tf: str, agg: int, before: int, limit: int) -> list[tuple]:
    doc = feeds.get_json(
        f"{feeds.GT}/networks/solana/pools/{pool}/ohlcv/{tf}?aggregate={agg}&limit={limit}"
        f"&currency=usd&before_timestamp={before}", feeds.GT_GATE, feeds.GT_HDR)
    rows = (((doc or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
    return sorted((float(t), float(o), float(h), float(lo), float(c), float(v or 0))
                  for t, o, h, lo, c, v in rows if c)


class SolPx:
    def __init__(self, t0: float, t1: float) -> None:
        self.px: dict[int, float] = {}
        before = int(t1)
        while before > t0:
            rows = ohlcv(SOL_USDC_POOL, "hour", 1, before, 1000)
            if not rows:
                break
            for r in rows:
                self.px[int(r[0] // 3600)] = r[4]
            before = int(rows[0][0])
            if len(rows) < 900:
                break
        self.last = st.median(self.px.values()) if self.px else 140.0

    def at(self, t: float) -> float:
        return self.px.get(int(t // 3600), self.last)


# ---------------- simulation ----------------

class Pool:
    """5-minute path of one graduate: price (SOL/token), volume (SOL), SOL reserve."""

    def __init__(self, sym: str, t_grad: float, rows: list, sol: SolPx, k: float, tiers: list) -> None:
        self.sym, self.t_grad = sym, t_grad
        self.t, self.p, self.vol, self.y = [], [], [], []
        for t, _o, _h, _lo, c, v in rows:
            s = sol.at(t)
            p = c / s
            self.t.append(t)
            self.p.append(p)
            self.vol.append(v / s)
            self.y.append((k * p) ** 0.5)
        self.vol1h = [sum(self.vol[max(0, i - 11):i + 1]) for i in range(len(self.vol))]
        self.lp_fee = [fees_at(tiers, p)[0] for p in self.p]
        self.tot_fee = [fees_at(tiers, p)[1] for p in self.p]


def lp_trade(pl: Pool, i0: int, size: float, rule: tuple) -> tuple[float, str, float] | None:
    """Return (net return on size, exit reason, hours held) for an LP entered at candle i0."""
    stop, trail, vdecay, fee_tgt, hold_h = rule
    n = len(pl.t)
    if i0 >= n - 2:
        return None
    y0, p0 = pl.y[i0], pl.p[i0]
    x0 = y0 / p0
    half = size / 2
    # buy `half` SOL of tokens: full fee, constant-product impact
    eff = half * (1 - pl.tot_fee[i0])
    tok = x0 * eff / (y0 + eff)
    y1, x1 = y0 + half, x0 - tok  # pool after our buy
    p1 = y1 / x1
    dep_sol = min(half, tok * p1)  # add both sides at the new ratio
    share = dep_sol / (y1 + dep_sol)
    k_after = (y1 + dep_sol) * (x1 + dep_sol / p1)
    fees = 0.0
    peak = p0
    vol_ref = sum(pl.vol[max(0, i0 - 12):i0]) or sum(pl.vol[i0:i0 + 12]) or 1e-9
    reason, j = "end", n - 1
    for j in range(i0 + 1, n):
        fees += pl.vol[j] * pl.lp_fee[j] * share
        p = pl.p[j]
        peak = max(peak, p)
        hrs = (pl.t[j] - pl.t[i0]) / 3600
        if stop and p < p0 * (1 - stop):
            reason = "stop"
        elif trail and p < peak * (1 - trail):
            reason = "trail"
        elif vdecay and j - i0 >= 12 and pl.vol1h[j] < vdecay * vol_ref:
            reason = "vol_decay"
        elif fee_tgt and fees >= fee_tgt * size:
            reason = "fee_target"
        elif hrs >= hold_h:
            reason = "time"
        else:
            continue
        j = min(j + 1, n - 1)  # filled one candle later
        break
    p = pl.p[j]
    y = (k_after * p) ** 0.5
    x = y / p
    sol_out, tok_out = share * y, share * x
    Y, X = y - sol_out, x - tok_out
    sold = Y * tok_out / (X + tok_out) * (1 - pl.tot_fee[j]) if X > 0 else 0.0
    final = sol_out + sold + fees - 4 * TX_SOL
    return final / size - 1, reason, (pl.t[j] - pl.t[i0]) / 3600


STOPS = (0.0, 0.15, 0.3, 0.5)
TRAILS = (0.0, 0.25, 0.4)
VDECAY = (0.0, 0.3)
FEE_TGT = (0.0, 0.05)
HOLDS = (2, 6, 24, 48)
RULES = list(itertools.product(STOPS, TRAILS, VDECAY, FEE_TGT, HOLDS))
ENTRY_DELAY_H = (0.0, 1.0, 4.0)
# Entry once the coin's market cap clears the tier where LPs earn 0.20% instead of 0.02%.
MCAP_GATE_SOL = 420.0
ENTRY_MODES = os.environ.get("LP_ENTRY", "mcap").split(",")


def summ(x: list[float]) -> str:
    if not x:
        return "n 0"
    return (f"n {len(x):3d} win {sum(v > 0 for v in x) / len(x):4.0%} mean {st.mean(x):+7.2%} "
            f"med {st.median(x):+7.2%} worst {min(x):+6.1%} p10 {sorted(x)[len(x) // 10]:+6.1%}")


def rule_name(r: tuple) -> str:
    stop, trail, vd, ft, h = r
    parts = [f"stop {stop:.0%}" if stop else "", f"trail {trail:.0%}" if trail else "",
             f"vol<{vd:.0%}" if vd else "", f"fees {ft:.0%}" if ft else "", f"max {h}h"]
    return ", ".join(p for p in parts if p)


def main() -> None:
    try:
        tiers = fee_tiers()
        print("PumpSwap fee tiers (mcap SOL >= : lp / protocol / creator bps):")
        for t in tiers:
            print(f"  {t[0] / 1e9:>12,.0f} SOL: {t[1]} / {t[2]} / {t[3]}")
    except Exception as e:  # noqa: BLE001
        print(f"fee config read failed ({type(e).__name__}: {e}); using 20/5/5 bps")
        tiers = [(0, 20, 5, 5)]

    rows = list(csv.DictReader(GRADS.open()))
    random.seed(7)
    sample = random.sample(rows, min(N_POOLS, len(rows)))
    sample.sort(key=lambda r: float(r["epoch"]))
    t_lo, t_hi = float(sample[0]["epoch"]), float(sample[-1]["epoch"]) + WINDOW_H * 3600
    sol = SolPx(t_lo - 3600, min(t_hi, time.time()))
    print(f"\nsample {len(sample)} graduations {time.strftime('%m-%d %H:%M', time.gmtime(t_lo))}"
          f" .. {time.strftime('%m-%d %H:%M', time.gmtime(float(sample[-1]['epoch'])))} UTC")

    pools: list[Pool] = []
    ys0, miss = [], 0
    for r in sample:
        tg = float(r["epoch"])
        info = pumpswap_pool(r["mint"])
        if not info or not info["pool"] or info["price_usd"] <= 0 or info["quote_usd"] <= 0:
            miss += 1
            continue
        c = ohlcv(info["pool"], "minute", 5, int(tg + WINDOW_H * 3600), 1000)
        c = [x for x in c if tg - 300 <= x[0] <= tg + WINDOW_H * 3600]
        if len(c) < 6:
            miss += 1
            continue
        p_now = info["price_usd"] / info["quote_usd"]
        y_now = info["liq"] / 2 / info["quote_usd"]
        if y_now <= 0:
            miss += 1
            continue
        k = y_now * y_now / p_now
        pl = Pool(info["sym"], tg, c, sol, k, tiers)
        ys0.append(pl.y[0])
        pools.append(pl)
    print(f"{len(pools)} pools with candles, {miss} without a PumpSwap pool or candles")
    if not pools:
        return
    print(f"implied SOL reserve at first candle: median {st.median(ys0):.0f} SOL "
          f"(pump.fun graduates start near 80; far off means LP was added or pulled)")
    dead = sum(min(p.p[len(p.p) // 2:] or p.p) < 0.1 * p.p[0] for p in pools)
    print(f"coins down 90%+ from graduation within {WINDOW_H} h: {dead}/{len(pools)}")
    print(f"LP fee at first candle: median {st.median(p.lp_fee[0] for p in pools):.2%}, "
          f"total fee {st.median(p.tot_fee[0] for p in pools):.2%}")

    cut = pools[int(len(pools) * 0.6)].t_grad
    modes = []
    if "delay" in ENTRY_MODES:
        modes += [(f"enter {d:g} h after graduation", "delay", d) for d in ENTRY_DELAY_H]
    if "mcap" in ENTRY_MODES:
        modes += [(f"enter once market cap >= {MCAP_GATE_SOL:.0f} SOL (LP fee 0.20%)", "mcap", 0.0),
                  (f"enter once market cap >= {MCAP_GATE_SOL:.0f} SOL and last-hour volume >= 200 SOL",
                   "mcap", 200.0)]
    for label, kind, arg in modes:
        print(f"\n################ {label}")
        starts = []
        for pl in pools:
            if kind == "delay":
                i0 = next((i for i, t in enumerate(pl.t) if t >= pl.t_grad + arg * 3600), None)
            else:
                i0 = next((i for i in range(1, len(pl.t))
                           if pl.p[i] * SUPPLY >= MCAP_GATE_SOL and pl.vol1h[i] >= arg), None)
            if i0 is not None:
                starts.append((pl, i0))
        print(f"{len(starts)} pools qualify")
        if len(starts) < 10:
            continue

        def run(rule: tuple, size: float) -> tuple[list, list, list]:
            tr, te, why = [], [], []
            for pl, i0 in starts:
                out = lp_trade(pl, i0, size, rule)
                if out:
                    (tr if pl.t_grad < cut else te).append(out[0])
                    why.append(out[1])
            return tr, te, why

        base = 0.5
        res = {rule: run(rule, base) for rule in RULES}
        ranked = sorted(RULES, key=lambda r: -st.mean(res[r][0] or [-1]))
        pos = sum(st.mean(res[r][0] or [-1]) > 0 for r in RULES)
        print(f"{pos}/{len(RULES)} exit rules positive on the older pools at {base} SOL")
        hold = (0.0, 0.0, 0.0, 0.0, 48)
        print("hold the LP 48 h, no exit rule:")
        print(f"  older {summ(res[hold][0])}\n  newer {summ(res[hold][1])}")
        for r in ranked[:6]:
            tr, te, why = res[r]
            mix = ", ".join(f"{k} {why.count(k)}" for k in sorted(set(why)))
            print(f"-- {rule_name(r)}   exits: {mix}")
            print(f"   older {summ(tr)}")
            print(f"   newer {summ(te)}")
        for r in ranked[:2]:
            print(f"deposit size, rule ({rule_name(r)}), older | newer:")
            for size in SIZES:
                tr, te, _ = run(r, size)
                print(f"   {size:>4} SOL  {summ(tr)}  |  {summ(te)}")
        te_means = [st.mean(res[r][1]) for r in RULES if res[r][1]]
        print(f"every rule on the newer pools: {sum(v > 0 for v in te_means)}/{len(te_means)} "
              f"positive, median rule {st.median(te_means):+.2%}, best {max(te_means):+.2%}")

if __name__ == "__main__":
    main()
