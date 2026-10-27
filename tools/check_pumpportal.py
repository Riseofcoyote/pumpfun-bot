"""Live check of the PumpPortal listener and the gate's stream watch. Moves no funds.

Runs the real listener for ~90s: prints raw create/trade messages (to confirm
field names), counts launches, and runs the quality gate's stream watch on the
first few SOL-looking coins with RPC holder lookups switched off.

    uv run tools/check_pumpportal.py
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import websockets  # noqa: E402

from monitoring import pumpportal_listener as pp  # noqa: E402
from trading.quality_gate import GateConfig, QualityGate  # noqa: E402


def note(text: str) -> None:
    """Print, and also emit a GitHub annotation so the result is readable via the API."""
    print(text)
    print("::notice::" + text.replace("%", "%25").replace("\n", " ")[:900])


async def raw_sample() -> None:
    """Subscribe to trades of the first 8 new coins and report what arrives in 40s."""
    async with websockets.connect(pp.PUMPPORTAL_WSS) as ws:
        await ws.send(json.dumps({"method": "subscribeNewToken"}))
        watched, trades, printed_create = set(), 0, False
        loop = asyncio.get_running_loop()
        end = loop.time() + 40
        while loop.time() < end:
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), 5))
            except TimeoutError:
                continue
            tx = msg.get("txType")
            if tx == "create" and len(watched) < 8:
                if not printed_create:
                    note("RAW CREATE: " + json.dumps(msg)[:850])
                    printed_create = True
                watched.add(msg["mint"])
                await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [msg["mint"]]}))
            elif tx in ("buy", "sell") and msg.get("mint") in watched:
                if trades == 0:
                    note("RAW TRADE: " + json.dumps(msg)[:850])
                trades += 1
            elif tx not in ("create", "buy", "sell"):
                note("OTHER MESSAGE: " + json.dumps(msg)[:300])
        note(f"trade messages for {len(watched)} watched coins in 40s: {trades}")


async def main() -> int:
    await raw_sample()
    listener = pp.PumpPortalListener()
    gate = QualityGate(GateConfig(observe_seconds=30), client=None, curve_manager=None)
    gate.holders_unavailable = True  # no RPC in this check
    seen, results = [], []

    counts = {}
    real_watch = listener.watch

    async def counting_watch(mint):
        w = await real_watch(mint)
        counts[str(mint)] = w
        return w

    listener.watch = counting_watch

    async def on_token(info):
        seen.append(info)
        gate.note_launch(info)
        if len(seen) <= 6:
            async def judge(i=info):
                r = await gate.evaluate_stream(i, listener)
                results.append((i.symbol, r))
            asyncio.create_task(judge())

    task = asyncio.create_task(listener.listen_for_tokens(on_token))
    await asyncio.sleep(75)
    task.cancel()
    note(f"launches received in 75s: {len(seen)}")
    for t in seen[:3]:
        print(f"  {t.symbol}: mint={t.mint} curve={t.bonding_curve} creator={t.creator}")
    v = [l.v_sol for l in list(listener.launches.values())[:10]]
    note(f"launch vSolInBondingCurve samples: {v}")
    for sym, r in results:
        note(f"gate {sym}: {'PASS' if r is None else 'skip - ' + r}")
    note("trades seen per gated coin: " + str([len(w.events) for w in counts.values()]))
    ok = len(seen) > 0 and all(29 <= x <= 120 for x in v if x)
    note(f"RESULT: {'OK' if ok else 'CHECK FAILED'}")
    return 0 if ok else 1


sys.exit(asyncio.run(main()))
