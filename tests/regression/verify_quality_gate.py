"""Verify the pre-buy quality gate's decisions and its use of the RPC replies.

Covers `trading/quality_gate.py`: the name and snapshot judges, holder share
excluding the bonding curve's own vault, dev-holding sums, the observation flow
(skip on a read failure, buy on a healthy coin) and the paper-trade CSV.
Offline: the client and curve manager are fakes. Exits non-zero on failure.
"""

import asyncio
import csv
import os
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from solders.pubkey import Pubkey

from core.pubkeys import WSOL_MINT
from interfaces.core import Platform, TokenInfo
from trading import quality_gate as qg

SUPPLY = qg.PUMP_TOTAL_SUPPLY
FAILS: list[str] = []


def check(cond: bool, label: str) -> None:
    print(("PASS " if cond else "FAIL ") + label)
    if not cond:
        FAILS.append(label)


def healthy(**kw) -> qg.Snapshot:
    base = dict(sol_start=0.5, sol_mid=3.0, sol_end=6.0, complete=False,
                top_holder_pct=0.04, top10_pct=0.2, dev_pct_start=0.02, dev_pct_end=0.02)
    base.update(kw)
    return qg.Snapshot(**base)


cfg = qg.GateConfig()
check(qg.judge_snapshot(healthy(), cfg) is None, "healthy coin passes")
check(qg.judge_snapshot(healthy(top_holder_pct=None, top10_pct=None), cfg) is None,
      "missing holder data passes when not required")
check("unavailable" in (qg.judge_snapshot(healthy(top_holder_pct=None, top10_pct=None),
      qg.GateConfig(require_holder_check=True)) or ""), "missing holder data skips when required")
check("raised" in qg.judge_snapshot(healthy(sol_end=2.0, sol_mid=1.0), cfg), "thin coin skipped")
check("too late" in qg.judge_snapshot(healthy(sol_mid=40, sol_end=50), cfg), "late coin skipped")
check("momentum" in qg.judge_snapshot(healthy(sol_mid=5.8, sol_end=6.0), cfg), "fading coin skipped")
check("top holder" in qg.judge_snapshot(healthy(top_holder_pct=0.2), cfg), "whale skipped")
check("top 10" in qg.judge_snapshot(healthy(top10_pct=0.5), cfg), "concentrated skipped")
check("dev holds" in qg.judge_snapshot(healthy(dev_pct_start=0.1, dev_pct_end=0.1), cfg), "dev bag skipped")
check("dev sold" in qg.judge_snapshot(healthy(dev_pct_start=0.04, dev_pct_end=0.0), cfg), "dev dump skipped")
check("complete" in qg.judge_snapshot(healthy(complete=True), cfg), "complete curve skipped")
check(qg.judge_name("RUGME", "x", cfg) is not None, "blocked word skipped")
check(qg.judge_name("A", "x", cfg) is not None, "1-char symbol skipped")
check(qg.judge_name("PEPE", "Pepe", cfg) is None, "normal name passes")
try:
    qg.GateConfig.from_dict({"min_sol_rasied": 3})
    check(False, "typo in config key rejected")
except ValueError:
    check(True, "typo in config key rejected")

MINT, CURVE, VAULT, DEV, WHALE = (Pubkey.new_unique() for _ in range(5))


class FakeCurve:
    def __init__(self, sols):
        self.sols, self.calls = list(sols), 0

    async def get_pool_state(self, pool, commitment=None):
        sol = self.sols[min(self.calls, len(self.sols) - 1)]
        self.calls += 1
        return {"real_sol_reserves": int(sol * qg.LAMPORTS), "complete": False}


class FakeClient:
    def __init__(self, holders, dev_amounts, fail_holders=False, balance=1.0):
        self.holders, self.dev, self.fail, self.balance = holders, list(dev_amounts), fail_holders, balance

    async def post_rpc(self, body, **_):
        m = body["method"]
        if m == "getBalance":
            return {"result": {"value": int(self.balance * qg.LAMPORTS)}}
        if m == "getTokenLargestAccounts":
            if self.fail:
                return None
            return {"result": {"value": [{"address": str(a), "amount": str(int(p * SUPPLY))}
                                         for a, p in self.holders]}}
        if m == "getAccountInfo":  # the creator's token account
            pct = self.dev.pop(0) if len(self.dev) > 1 else self.dev[0]
            if pct == 0:
                return {"result": {"value": None}}
            amt = str(int(pct * SUPPLY))
            return {"result": {"value": {"data": {"parsed": {"info": {
                "tokenAmount": {"amount": amt}}}}}}}
        raise AssertionError(m)


def token(**kw) -> TokenInfo:
    base = dict(name="Pepe", symbol="PEPE", uri="", mint=MINT, platform=Platform.PUMP_FUN,
                bonding_curve=CURVE, associated_bonding_curve=VAULT, creator=DEV,
                quote_mint=WSOL_MINT)
    base.update(kw)
    return TokenInfo(**base)


fast = qg.GateConfig(observe_seconds=0.01)
# the curve vault holds 70% and must be ignored; the biggest real holder has 5%
holders = [(VAULT, 0.70), (WHALE, 0.05), (Pubkey.new_unique(), 0.03)]
gate = qg.QualityGate(fast, FakeClient(holders, [0.02]), FakeCurve([0.5, 3.0, 6.0]))
check(asyncio.run(gate.evaluate(token(), CURVE)) is None, "evaluate buys a healthy coin (curve vault excluded)")

gate = qg.QualityGate(fast, FakeClient(holders, [0.04, 0.0]), FakeCurve([0.5, 3.0, 6.0]))
r = asyncio.run(gate.evaluate(token(), CURVE))
check(r is not None and "dev sold" in r, "evaluate catches dev selling mid-window")

gate = qg.QualityGate(fast, FakeClient(holders, [0.02], fail_holders=True), FakeCurve([0.5, 3.0, 6.0]))
r = asyncio.run(gate.evaluate(token(), CURVE))
check(r is None and gate.holders_unavailable, "refused holder lookup: trade on, check switched off")


class DeadClient(FakeClient):
    async def post_rpc(self, body, **kw):
        return None if body["method"] == "getAccountInfo" else await super().post_rpc(body, **kw)

gate = qg.QualityGate(fast, DeadClient(holders, [0.02]), FakeCurve([0.5, 3.0, 6.0]))
r = asyncio.run(gate.evaluate(token(), CURVE))
check(r is not None and "could not measure" in r, "dev lookup failure means no buy")

gate = qg.QualityGate(fast, FakeClient(holders, [0.02]), FakeCurve([0.5, 3, 6]))
check(gate.quick_reject(token(quote_mint=Pubkey.new_unique())) == "not SOL-paired", "non-SOL pair skipped")
t = token()
gate.note_launch(t); gate.note_launch(t)
check(gate.quick_reject(t) is None, "one coin reported twice is not a serial launcher")
gate.note_launch(token(mint=Pubkey.new_unique()))
check("serial" in (gate.quick_reject(t) or ""), "creator with two coins skipped as serial launcher")
check(asyncio.run(qg.QualityGate(fast, FakeClient(holders, [0], balance=0.01), None)
                  .wallet_below_floor(DEV)), "wallet floor stops buying")
check(not asyncio.run(qg.QualityGate(fast, FakeClient(holders, [0], balance=1), None)
                      .wallet_below_floor(DEV)), "wallet above floor keeps buying")

# stream path (PumpPortal): SOL raised, distinct buyers and dev sells come from trades
from dataclasses import dataclass as _dc  # noqa: E402


@_dc
class _Launch:
    creator: str
    initial_buy_tokens: float
    v_sol: float


class FakeWatch:
    def __init__(self, events):
        self.events, self._all = [], list(events)

    @property
    def last_v_sol(self):
        return self.events[-1]["vSolInBondingCurve"] if self.events else None


class FakeStream:
    """Releases the first half of the trades at watch(), the rest at the midpoint read."""

    def __init__(self, launch, first, second):
        self.launches = {str(MINT): launch}
        self.first, self.second, self.unwatched = first, second, False

    async def watch(self, mint):
        self.w = FakeWatch([])
        self.w.events.extend(self.first)
        asyncio.get_running_loop().call_later(0.007, self.w.events.extend, self.second)
        return self.w

    async def unwatch(self, mint):
        self.unwatched = True


DEV = str(Pubkey.new_unique())


def buys(n, v0, step):
    return [{"txType": "buy", "traderPublicKey": f"b{i}", "vSolInBondingCurve": v0 + step * (i + 1)}
            for i in range(n)]

launch = _Launch(DEV, 20_000_000, 30.5)  # dev bought 2% at launch
stream = FakeStream(launch, buys(6, 30.5, 0.5), buys(8, 33.5, 0.4))
gate = qg.QualityGate(fast, FakeClient(holders, [0.02]), None)
r = asyncio.run(gate.evaluate_stream(token(creator=Pubkey.from_string(DEV)), stream))
check(r is None and stream.unwatched, "stream: healthy coin passes and trades are unsubscribed")

stream = FakeStream(launch, buys(3, 30.5, 1.5), buys(2, 35.0, 0.5))
r = asyncio.run(qg.QualityGate(fast, FakeClient(holders, [0.02]), None)
                .evaluate_stream(token(creator=Pubkey.from_string(DEV)), stream))
check(r is not None and "distinct buyers" in r, "stream: too few distinct buyers skipped")

dev_sell = [{"txType": "sell", "traderPublicKey": DEV, "newTokenBalance": 0, "vSolInBondingCurve": 36.0}]
stream = FakeStream(launch, buys(6, 30.5, 0.5), buys(8, 33.5, 0.4) + dev_sell)
r = asyncio.run(qg.QualityGate(fast, FakeClient(holders, [0.02]), None)
                .evaluate_stream(token(creator=Pubkey.from_string(DEV)), stream))
check(r is not None and "dev sold" in r, "stream: dev selling in the window skipped")

# RPC watch seeded from a PumpPortal create: start values free, 2 curve reads
class CountingClient(FakeClient):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.methods = []

    async def post_rpc(self, body, **kw):
        self.methods.append(body["method"])
        return await super().post_rpc(body, **kw)


c = CountingClient(holders, [0.02])
curve = FakeCurve([3.0, 6.0])
r = asyncio.run(qg.QualityGate(fast, c, curve).evaluate(
    token(creator=Pubkey.new_unique()), CURVE, launch=launch))
check(r is None and curve.calls == 2, "seeded RPC watch: healthy coin passes with 2 curve reads")

c = CountingClient(holders, [0.02])
curve = FakeCurve([0.4, 0.6])
r = asyncio.run(qg.QualityGate(fast, c, curve).evaluate(
    token(creator=Pubkey.new_unique()), CURVE, launch=launch))
check(r is not None and "raised" in r and c.methods == [],
      "seeded RPC watch: thin coin skipped without dev/holder lookups")

# paper trade: price rises 60% -> take profit, net of 3% costs
prices = iter([1.0, 1.2, 1.6])


async def read_price(_):
    return next(prices)

with tempfile.TemporaryDirectory() as d:
    cwd = os.getcwd()
    os.chdir(d)
    try:
        pg = qg.QualityGate(qg.GateConfig(paper_check_interval=0), FakeClient([], [0]), None)
        asyncio.run(pg.paper_trade(token(), read_price))
        rows = list(csv.DictReader(open("trades/paper_trades.csv")))
    finally:
        os.chdir(cwd)
check(rows[0]["outcome"] == "take_profit" and rows[0]["net_pnl_pct"] == "57.0",
      "paper trade logs take-profit net of costs")

# First-half growth and the curve transaction count.
c2 = qg.GateConfig(min_sol_growth_first_half=1.0, min_curve_txs=25, max_avg_sol_per_tx=0.5)
check("slow start" in (qg.judge_snapshot(healthy(sol_start=2.8, sol_mid=3.0), c2) or ""),
      "flat first half skipped")
check(qg.judge_snapshot(healthy(curve_txs=None), c2) is None, "no tx count passes")
check(qg.judge_snapshot(healthy(curve_txs=40), c2) is None, "busy curve passes")
check("curve transactions" in (qg.judge_snapshot(healthy(curve_txs=10), c2) or ""),
      "quiet curve skipped")
check("big buyers" in (qg.judge_snapshot(healthy(sol_end=20.0, sol_mid=10.0, curve_txs=26), c2) or ""),
      "few big buys skipped")

# Trailing stop on the position.
from trading.position import ExitReason, Position  # noqa: E402

pos = Position.create_from_buy_result(
    Pubkey.default(), "X", 1.0, 1.0, take_profit_percentage=1.0, stop_loss_percentage=0.2,
    max_hold_time=180, trail_activation_pct=0.2, trail_distance_pct=0.15, trail_floor_pct=0.05,
)
check(pos.should_exit(1.15) == (False, None), "trail not armed below +20%")
check(pos.should_exit(1.0) == (False, None), "unarmed trail does not fire at entry")
check(pos.should_exit(1.5) == (False, None), "new high holds")
check(pos.should_exit(1.3) == (False, None), "dip inside 15% of the high holds")
check(pos.should_exit(1.27)[1] is ExitReason.TRAILING_STOP, "15% off the high sells")
pos2 = Position.create_from_buy_result(
    Pubkey.default(), "Y", 1.0, 1.0, stop_loss_percentage=0.2,
    trail_activation_pct=0.2, trail_distance_pct=0.15, trail_floor_pct=0.05,
)
pos2.should_exit(1.21)
check(pos2.trailing_stop_price() == 1.05, "floor keeps the stop at +5% after a small run")
check(pos2.should_exit(1.04)[1] is ExitReason.TRAILING_STOP, "a +20% coin never closes as a loss")
plain = Position.create_from_buy_result(Pubkey.default(), "Z", 1.0, 1.0, stop_loss_percentage=0.2)
check(plain.should_exit(0.85) == (False, None) and plain.should_exit(0.8)[1] is ExitReason.STOP_LOSS,
      "no trail configured: plain stop loss")

# Launch block and funder tracing (trading/launch_forensics.py).
from trading import launch_forensics as lf  # noqa: E402

sigs = [  # newest first, as getSignaturesForAddress returns them
    {"signature": "late", "slot": 105, "err": None, "blockTime": 2},
    {"signature": "b3", "slot": 100, "err": None},
    {"signature": "bad", "slot": 100, "err": {"x": 1}},
    {"signature": "b2", "slot": 100, "err": None},
    {"signature": "create", "slot": 100, "err": None, "blockTime": 1},
]
blk = lf.find_launch_block(sigs, "create")
check(blk is not None and blk.buys == 2 and blk.slot == 100, "launch block counts other ok txs in the create slot")
check(lf.find_launch_block(sigs, "").buys == 2, "without the create sig the oldest entry stands in")
check(lf.find_launch_block([{"signature": f"s{i}", "slot": i, "err": None} for i in range(1000)], "") is None,
      "a full page without the create sig is unknown, not clean")
check(lf.find_launch_block(sigs[:1], "late").buys == 0, "lone create is a clean launch")

c3 = qg.GateConfig(max_launch_block_buys=2)
check(qg.judge_snapshot(healthy(launch_block_buys=2), c3) is None, "2 snipers in the launch block pass")
check("bundled" in (qg.judge_snapshot(healthy(launch_block_buys=3), c3) or ""), "3+ launch-block buys skipped")
check(qg.judge_snapshot(healthy(launch_block_buys=9), cfg) is None, "launch-block check off by default")


def transfer_tx(src, dst, sol, inner=False):
    ix = {"program": "system", "parsed": {"type": "transfer", "info": {
        "source": src, "destination": dst, "lamports": int(sol * qg.LAMPORTS)}}}
    msg = {"instructions": [] if inner else [ix], "accountKeys": [{"pubkey": src, "signer": True}]}
    meta = {"err": None, "innerInstructions": [{"index": 0, "instructions": [ix]}] if inner else []}
    return {"transaction": {"message": msg}, "meta": meta}


check(lf.funding_transfer(transfer_tx("F", "W", 0.5), "W") == ("F", 0.5), "funding transfer found")
check(lf.funding_transfer(transfer_tx("F", "W", 0.5, inner=True), "W")[0] == "F", "inner-instruction funding found")
check(lf.funding_transfer(transfer_tx("F", "W", 0.001), "W") is None, "dust is not funding")
check(lf.fee_payer(transfer_tx("F", "W", 1)) == "F", "fee payer is the first signer")

mem = lf.CrewMemory(None)
check("dev funded" in (lf.judge_funding("M", "D", {"D": "X", "B1": "D"}, mem, max_shared_funder_wallets=1, max_crew_earlier_launches=0) or ""), "dev funding a buyer skipped")
check("linked" in (lf.judge_funding("M", "D", {"D": "F", "B1": "F", "B2": "G"}, mem, max_shared_funder_wallets=1, max_crew_earlier_launches=0) or ""),
      "two launch wallets on one funder skipped")
check(lf.judge_funding("M", "D", {"D": "F", "B1": "G"}, mem, max_shared_funder_wallets=1, max_crew_earlier_launches=0) is None, "independent funders pass")
ex = next(iter(lf.EXCHANGES))
check(lf.judge_funding("M", "D", {"D": ex, "B1": ex, "B2": ex}, mem, max_shared_funder_wallets=1, max_crew_earlier_launches=0) is None,
      "shared exchange funder links nobody")
mem.record("OLD", {"D0": "F"})
check("repeat crew" in (lf.judge_funding("M", "D", {"D": "F"}, mem, max_shared_funder_wallets=1, max_crew_earlier_launches=0) or ""), "funder behind an earlier coin skipped")
check(lf.judge_funding("M", "D", {"D": "F"}, mem, max_shared_funder_wallets=1, max_crew_earlier_launches=-1) is None, "crew memory check can be turned off")
hub = lf.CrewMemory(None, hub_wallets=3)
hub.record("A", {"w1": "H", "w2": "H", "w3": "H"})
check(lf.judge_funding("M", "D", {"D": "H", "B": "H"}, hub, max_shared_funder_wallets=1, max_crew_earlier_launches=0) is None, "hub funder treated as a service")
with tempfile.TemporaryDirectory() as d:
    path = Path(d) / "crews.json"
    lf.CrewMemory(path).record("OLD", {"D0": "F"})
    check(lf.CrewMemory(path).earlier_launches("F", "NEW") == ["OLD"], "crew memory survives a restart")


class ForensicClient(FakeClient):
    """Curve history with a bundled launch block; wallets funded as ``funders`` says."""

    def __init__(self, *a, curve_sigs, payers, funders, **k):
        super().__init__(*a, **k)
        self.curve_sigs, self.payers, self.funders, self.calls = curve_sigs, payers, funders, []

    async def post_rpc(self, body, **kw):
        m, p = body["method"], body["params"]
        self.calls.append(m)
        if m == "getSignaturesForAddress":
            if p[0] == str(CURVE):
                return {"result": self.curve_sigs}
            return {"result": [{"signature": f"fund-{p[0]}", "err": None}]}
        if m == "getTransaction":
            sig = p[0]
            if sig.startswith("fund-"):
                w = sig[5:]
                return {"result": transfer_tx(self.funders[w], w, 1.0)} if w in self.funders else {"result": None}
            return {"result": transfer_tx(self.payers[sig], "curve", 0.5)}
        return await super().post_rpc(body, **kw)


creator = Pubkey.new_unique()
curve_sigs = [{"signature": "b1", "slot": 7, "err": None, "blockTime": None},
              {"signature": "b2", "slot": 7, "err": None, "blockTime": None},
              {"signature": "create", "slot": 7, "err": None, "blockTime": None}]
seeded = _Launch(str(creator), 20_000_000, 30.5)
seeded.signature = "create"
fcfg = replace(fast, max_launch_block_buys=2, trace_funders=True, crew_memory_path="")
fc = ForensicClient(holders, [0.02], curve_sigs=curve_sigs, payers={"b1": "W1", "b2": "W2"},
                    funders={"W1": "BOSS", "W2": "BOSS", str(creator): "DEVF"})
r = asyncio.run(qg.QualityGate(fcfg, fc, FakeCurve([3.0, 6.0])).evaluate(
    token(creator=creator), CURVE, launch=seeded))
check(r is not None and "linked" in r, "evaluate: launch-block buyers on one funder skipped")

fc = ForensicClient(holders, [0.02], curve_sigs=curve_sigs, payers={"b1": "W1", "b2": "W2"},
                    funders={"W1": "A", "W2": "B", str(creator): "C"})
g = qg.QualityGate(fcfg, fc, FakeCurve([3.0, 6.0]))
r = asyncio.run(g.evaluate(token(creator=creator), CURVE, launch=seeded))
check(r is None and fc.calls.count("getTransaction") == 5, "evaluate: independent launch wallets pass")
check(g.crews.earlier_launches("C", "next-coin") == [str(MINT)], "evaluate: traced funders remembered")

fc = ForensicClient(holders, [0.02], curve_sigs=curve_sigs, payers={}, funders={})
r = asyncio.run(qg.QualityGate(fcfg, fc, FakeCurve([0.4, 0.6])).evaluate(
    token(creator=creator), CURVE, launch=seeded))
check(r is not None and "getTransaction" not in fc.calls, "evaluate: thin coin skipped before any tracing")


sys.exit(1 if FAILS else 0)
