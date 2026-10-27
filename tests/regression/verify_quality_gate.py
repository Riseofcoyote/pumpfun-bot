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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from solders.pubkey import Pubkey  # noqa: E402

from core.pubkeys import WSOL_MINT  # noqa: E402
from interfaces.core import Platform, TokenInfo  # noqa: E402
from trading import quality_gate as qg  # noqa: E402

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

sys.exit(1 if FAILS else 0)
