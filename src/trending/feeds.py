"""Market data for the trending paper engine: GeckoTerminal and DexScreener.

Stdlib only (urllib), so the engine runs without the Solana stack. Every
function returns ``None`` or an empty result on a network or parse failure;
the engine treats a missing read as "no data this tick", never as a price.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime

logger = logging.getLogger("trending.feeds")

GT = "https://api.geckoterminal.com/api/v2"
DS = "https://api.dexscreener.com/latest/dex"
UA = "Mozilla/5.0 (compatible; paper-research-bot/1.0)"

# Majors and stables: high volume but not what this engine trades.
EXCLUDED_SYMBOLS = {
    "SOL",
    "WSOL",
    "USDC",
    "USDT",
    "PYUSD",
    "USDS",
    "USD1",
    "JUP",
    "JTO",
    "JITOSOL",
    "MSOL",
    "BSOL",
    "INF",
    "JLP",
    "WBTC",
    "CBBTC",
    "WETH",
    "ETH",
    "RAY",
    "ORCA",
    "PYTH",
    "W",
    "HNT",
    "RENDER",
    "EURC",
    "FDUSD",
    "USDG",
    "SSOL",
    "HSOL",
    "BNSOL",
}
SOL_MINT = "So11111111111111111111111111111111111111112"


class RateGate:
    """Spaces requests to one host so a burst cannot trip its rate limit."""

    def __init__(self, per_minute: int) -> None:
        self.gap = 60.0 / per_minute
        self.next_ok = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        if now < self.next_ok:
            time.sleep(self.next_ok - now)
        self.next_ok = max(now, self.next_ok) + self.gap


GT_GATE = RateGate(25)  # free tier allows 30/min
DS_GATE = RateGate(240)  # pairs endpoint allows 300/min


def get_json(url: str, gate: RateGate, headers: dict | None = None) -> dict | None:
    """GET a JSON document; ``None`` after 3 failed attempts."""
    hdrs = {"User-Agent": UA, "Accept": "application/json"}
    hdrs.update(headers or {})
    for attempt in range(3):
        gate.wait()
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=12) as r:  # noqa: S310
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(10 * (attempt + 1))
                continue
            logger.warning(f"HTTP {e.code} for {url.split('?')[0]}")
            if e.code in (400, 404):
                return None
        except (urllib.error.URLError, TimeoutError, ValueError, OSError) as e:
            logger.warning(f"fetch failed ({type(e).__name__}) {url.split('?')[0]}")
        time.sleep(1 + attempt * 2)
    return None


def _f(x: object, default: float = 0.0) -> float:
    try:
        return float(x)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _parse_gt_pool(p: dict) -> dict | None:
    a = p.get("attributes") or {}
    rel = p.get("relationships") or {}
    base_id = ((rel.get("base_token") or {}).get("data") or {}).get("id", "")
    quote_id = ((rel.get("quote_token") or {}).get("data") or {}).get("id", "")
    dex = ((rel.get("dex") or {}).get("data") or {}).get("id", "")
    mint = base_id.split("_", 1)[-1]
    quote = quote_id.split("_", 1)[-1]
    name = a.get("name") or ""
    symbol = name.split(" / ")[0].strip()
    if not mint or symbol.upper() in EXCLUDED_SYMBOLS or mint == SOL_MINT:
        return None
    price = _f(a.get("base_token_price_usd"))
    if price <= 0 or _f(a.get("reserve_in_usd")) < 3_000:
        return None  # nothing tradeable at any size
    created = a.get("pool_created_at")
    age_h = 9999.0
    if created:
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            age_h = (datetime.now(UTC) - dt).total_seconds() / 3600
        except ValueError:
            pass
    pc = a.get("price_change_percentage") or {}
    tx = a.get("transactions") or {}
    vol = a.get("volume_usd") or {}

    def txw(w: str) -> dict:
        t = tx.get(w) or {}
        return {k: int(_f(t.get(k))) for k in ("buys", "sells", "buyers", "sellers")}

    return {
        "pool": a.get("address") or p.get("id", "").split("_", 1)[-1],
        "mint": mint,
        "symbol": symbol[:20],
        "dex": dex,
        "quote_is_sol": quote == SOL_MINT,
        "price": price,
        "liq": _f(a.get("reserve_in_usd")),
        "mcap": _f(a.get("market_cap_usd")) or _f(a.get("fdv_usd")),
        "age_h": age_h,
        "pc": {w: _f(pc.get(w)) / 100 for w in ("m5", "m15", "m30", "h1", "h6", "h24")},
        "tx": {w: txw(w) for w in ("m5", "m15", "h1", "h24")},
        "vol": {w: _f(vol.get(w)) for w in ("m5", "m15", "h1", "h6", "h24")},
    }


GT_HDR = {"Accept": "application/json;version=20230302"}


def discover() -> dict[str, dict]:
    """Snapshot of trending + top-volume Solana pools, keyed by pool address.

    Each record also carries ``ranks``: list name -> 1-based position.
    """
    lists = {
        "trend5m": f"{GT}/networks/solana/trending_pools?duration=5m&page=1",
        "trend5m_p2": f"{GT}/networks/solana/trending_pools?duration=5m&page=2",
        "trend1h": f"{GT}/networks/solana/trending_pools?duration=1h&page=1",
        "volume": f"{GT}/networks/solana/pools?sort=h24_volume_usd_desc&page=1",
        "volume_p2": f"{GT}/networks/solana/pools?sort=h24_volume_usd_desc&page=2",
    }
    out: dict[str, dict] = {}
    for name, url in lists.items():
        doc = get_json(url, GT_GATE, GT_HDR)
        if not doc:
            continue
        offset = 20 if name.endswith("_p2") else 0
        key = name.removesuffix("_p2")
        for i, p in enumerate(doc.get("data") or []):
            rec = _parse_gt_pool(p)
            if not rec:
                continue
            cur = out.setdefault(rec["pool"], rec | {"ranks": {}})
            cur["ranks"].setdefault(key, offset + i + 1)
    # A token can trend in several pools; keep its deepest pool only.
    best: dict[str, dict] = {}
    for rec in out.values():
        b = best.get(rec["mint"])
        if b is None or rec["liq"] > b["liq"]:
            if b is not None:
                for k, v in b["ranks"].items():
                    rec["ranks"].setdefault(k, v)
            best[rec["mint"]] = rec
        else:
            for k, v in rec["ranks"].items():
                b["ranks"].setdefault(k, v)
    return {r["pool"]: r for r in best.values()}


JUP = "https://lite-api.jup.ag/price/v3"
JUP_GATE = RateGate(50)


def jup_prices(mints: list[str]) -> dict[str, float]:
    """USD price per mint from Jupiter's router (route-weighted, near real time)."""
    out: dict[str, float] = {}
    for i in range(0, len(mints), 50):
        doc = get_json(f"{JUP}?ids={','.join(mints[i : i + 50])}", JUP_GATE)
        for mint, v in (doc or {}).items():
            px = _f((v or {}).get("usdPrice")) if isinstance(v, dict) else 0.0
            if px > 0:
                out[mint] = px
    return out


def quotes(pairs: dict[str, str]) -> dict[str, dict]:
    """Latest price, liquidity and 5-minute flow per pool.

    ``pairs`` maps pool address -> token mint. Price comes from Jupiter when it
    has one (``src`` "jup"), else DexScreener ("ds"); liquidity and flow always
    come from DexScreener. Pools DexScreener does not return are absent.
    """
    pools = list(pairs)
    out: dict[str, dict] = {}
    for i in range(0, len(pools), 30):
        chunk = pools[i : i + 30]
        url = f"{DS}/pairs/solana/{','.join(urllib.parse.quote(p) for p in chunk)}"
        doc = get_json(url, DS_GATE)
        if not doc:
            continue
        for pr in doc.get("pairs") or []:
            addr = pr.get("pairAddress")
            price = _f(pr.get("priceUsd"))
            if not addr or price <= 0:
                continue
            m5 = (pr.get("txns") or {}).get("m5") or {}
            out[addr] = {
                "price": price,
                "src": "ds",
                "liq": _f((pr.get("liquidity") or {}).get("usd")),
                "buys_m5": int(_f(m5.get("buys"))),
                "sells_m5": int(_f(m5.get("sells"))),
            }
    jp = jup_prices(sorted({pairs[p] for p in out}))
    for pool, q in out.items():
        px = jp.get(pairs[pool])
        # Ignore a router price far off the pool's own; it is a different market.
        if px and 0.7 < px / q["price"] < 1.43:
            q["price"], q["src"] = px, "jup"
    return out


def sol_usd() -> float | None:
    """SOL price in USD, for sizing paper trades in SOL."""
    doc = get_json(
        f"{GT}/simple/networks/solana/token_price/{SOL_MINT}", GT_GATE, GT_HDR
    )
    try:
        return float(doc["data"]["attributes"]["token_prices"][SOL_MINT])  # type: ignore[index]
    except (TypeError, KeyError, ValueError):
        return None
