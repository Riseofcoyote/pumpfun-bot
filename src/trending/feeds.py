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


def get_json(
    url: str, gate: RateGate, headers: dict | None = None
) -> dict | list | None:
    """GET a JSON document (an object, or a list on some endpoints); ``None`` after 3 failed attempts."""
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
            logger.warning(f"HTTP {e.code} for {url.split('?', maxsplit=1)[0]}")
            if e.code in (400, 404):
                return None
        except (urllib.error.URLError, TimeoutError, ValueError, OSError) as e:
            logger.warning(
                f"fetch failed ({type(e).__name__}) {url.split('?', maxsplit=1)[0]}"
            )
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


def discover(
    pumpswap_pages: int = 0, grad_pools: list[str] | None = None
) -> dict[str, dict]:
    """Snapshot of trending + top-volume Solana pools, keyed by pool address.

    Each record also carries ``ranks``: list name -> 1-based position.
    ``pumpswap_pages`` adds that many pages of PumpSwap's own 24 h volume list
    (rank key ``pumpvol``); each page is one more GeckoTerminal call, which
    shares the per-IP limit with every tester on the machine. ``grad_pools``
    (pool addresses, at most 30 used) adds those pools under rank key ``grad``,
    ranked by 24 h volume, in one more call.
    """
    lists = {
        "trend5m": f"{GT}/networks/solana/trending_pools?duration=5m&page=1",
        "trend5m_p2": f"{GT}/networks/solana/trending_pools?duration=5m&page=2",
        "trend1h": f"{GT}/networks/solana/trending_pools?duration=1h&page=1",
        "trend1h_p2": f"{GT}/networks/solana/trending_pools?duration=1h&page=2",
        "trend6h": f"{GT}/networks/solana/trending_pools?duration=6h&page=1",
        "volume": f"{GT}/networks/solana/pools?sort=h24_volume_usd_desc&page=1",
        "volume_p2": f"{GT}/networks/solana/pools?sort=h24_volume_usd_desc&page=2",
        "volume_p3": f"{GT}/networks/solana/pools?sort=h24_volume_usd_desc&page=3",
    }
    for page in range(1, pumpswap_pages + 1):
        name = "pumpvol" if page == 1 else f"pumpvol_p{page}"
        lists[name] = (
            f"{GT}/networks/solana/dexes/pumpswap/pools"
            f"?sort=h24_volume_usd_desc&page={page}"
        )
    out: dict[str, dict] = {}
    for name, url in lists.items():
        doc = get_json(url, GT_GATE, GT_HDR)
        if not doc:
            continue
        key, _, page = name.partition("_p")
        offset = 20 * (int(page) - 1) if page else 0
        for i, p in enumerate(doc.get("data") or []):
            rec = _parse_gt_pool(p)
            if not rec:
                continue
            cur = out.setdefault(rec["pool"], rec | {"ranks": {}})
            cur["ranks"].setdefault(key, offset + i + 1)
    if grad_pools:
        recs = gt_pools(grad_pools[:30])
        ordered = sorted(recs.values(), key=lambda r: -r["vol"]["h24"])
        for i, rec in enumerate(ordered):
            cur = out.setdefault(rec["pool"], rec | {"ranks": {}})
            cur["ranks"].setdefault("grad", i + 1)
    return _deepest_per_mint(out.values())


def gt_pools(addrs: list[str]) -> dict[str, dict]:
    """GeckoTerminal records for the given pool addresses, 30 per call."""
    out: dict[str, dict] = {}
    for i in range(0, len(addrs), 30):
        chunk = ",".join(addrs[i : i + 30])
        doc = get_json(f"{GT}/networks/solana/pools/multi/{chunk}", GT_GATE, GT_HDR)
        for p in (doc or {}).get("data") or []:
            rec = _parse_gt_pool(p)
            if rec:
                out[rec["pool"]] = rec
    return out


DS_TOKENS = "https://api.dexscreener.com/tokens/v1/solana"


def pumpswap_pools_of(
    mints: list[str], min_liq: float, min_age_h: float, max_age_h: float
) -> list[str]:
    """Deepest PumpSwap pool of each mint whose age and USD liquidity pass.

    Ages are in hours from DexScreener's ``pairCreatedAt``. Returned deepest
    first; mints DexScreener has no PumpSwap pair for are left out.
    """
    best: dict[str, tuple[float, str]] = {}
    now_ms = time.time() * 1000
    for i in range(0, len(mints), 30):
        doc = get_json(f"{DS_TOKENS}/{','.join(mints[i : i + 30])}", DS_GATE)
        pairs = doc if isinstance(doc, list) else (doc or {}).get("pairs") or []
        for pr in pairs:
            if not isinstance(pr, dict) or pr.get("dexId") != "pumpswap":
                continue
            mint = (pr.get("baseToken") or {}).get("address")
            liq = _f((pr.get("liquidity") or {}).get("usd"))
            created = _f(pr.get("pairCreatedAt"))
            if not mint or not pr.get("pairAddress") or liq < min_liq or created <= 0:
                continue
            if not min_age_h <= (now_ms - created) / 3.6e6 <= max_age_h:
                continue
            if liq > best.get(mint, (0.0, ""))[0]:
                best[mint] = (liq, pr["pairAddress"])
    return [a for _, a in sorted(best.values(), reverse=True)]


def _deepest_per_mint(recs) -> dict[str, dict]:  # noqa: ANN001
    """Keep each token's deepest pool, carrying over ranks from its other pools."""
    best: dict[str, dict] = {}
    for rec in recs:
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


# Mint -> decimals, filled from Jupiter price reads; sizes an exit sell quote.
DECIMALS: dict[str, int] = {}


def jup_prices(mints: list[str]) -> dict[str, float]:
    """USD price per mint from Jupiter's router (route-weighted, near real time)."""
    out: dict[str, float] = {}
    for i in range(0, len(mints), 50):
        doc = get_json(f"{JUP}?ids={','.join(mints[i : i + 50])}", JUP_GATE)
        for mint, v in (doc or {}).items():
            px = _f((v or {}).get("usdPrice")) if isinstance(v, dict) else 0.0
            if px > 0:
                out[mint] = px
            dec = v.get("decimals") if isinstance(v, dict) else None
            if isinstance(dec, int) and 0 <= dec <= 12:
                DECIMALS[mint] = dec
    return out


JUP_QUOTE = "https://lite-api.jup.ag/swap/v1/quote"


def jup_sell_lamports(mint: str, raw_amount: int) -> int | None:
    """Lamports Jupiter's router would pay for ``raw_amount`` base units of ``mint``.

    An executable quote, so pool fees and price impact are included; ``None`` when
    there is no route or the call fails.
    """
    if raw_amount <= 0:
        return None
    url = (
        f"{JUP_QUOTE}?inputMint={mint}&outputMint={SOL_MINT}&amount={raw_amount}"
        "&slippageBps=500&restrictIntermediateTokens=true"
    )
    doc = get_json(url, JUP_GATE)
    out = int(_f((doc or {}).get("outAmount")))
    return out if out > 0 else None


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
