"""
kalshi_client.py
================
Authenticated Kalshi REST API client with RSA-PSS signing.
Supports DEMO (demo-api.kalshi.co) and LIVE (trading-api.kalshi.com).

BUG FIX (v2): RSA signature must cover the FULL path /trade-api/v2/...
  WRONG: sign("GET", "/portfolio/balance")
  RIGHT: sign("GET", "/trade-api/v2/portfolio/balance")

Author: APEX Trading Suite - Kalshi Module
"""

import time
import base64
import logging
import os
from typing import Optional, Dict, Any, List

import requests
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger("KalshiClient")


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────
API_PREFIX    = "/trade-api/v2"
DEMO_HOST     = "https://demo-api.kalshi.co"
LIVE_HOST     = "https://trading-api.kalshi.com"
DEMO_BASE_URL = DEMO_HOST + API_PREFIX
LIVE_BASE_URL = LIVE_HOST + API_PREFIX
DEMO_WS_URL   = "wss://demo-api.kalshi.co/trade-api/ws/v2"
LIVE_WS_URL   = "wss://trading-api.kalshi.com/trade-api/ws/v2"


class KalshiAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"[{status_code}] {message}")


class KalshiClient:
    """
    Authenticated Kalshi API client using RSA-PSS signed requests.

    IMPORTANT — how signing works:
      Every request includes three custom headers computed from:
        message = timestamp_ms + METHOD_UPPER + FULL_PATH
      where FULL_PATH = /trade-api/v2 + endpoint
      e.g. "/trade-api/v2/portfolio/balance"
    """

    def __init__(self, key_id: str, private_key_path: str, mode: str = "demo"):
        self.key_id   = key_id.strip()
        self.mode     = mode.lower()
        self.base_url = DEMO_BASE_URL if self.mode == "demo" else LIVE_BASE_URL
        self.ws_url   = DEMO_WS_URL   if self.mode == "demo" else LIVE_WS_URL

        # Load private key ─────────────────────────────────────────────────
        key_path = os.path.expanduser(private_key_path)
        if not os.path.exists(key_path):
            raise FileNotFoundError(
                f"\n❌ Private key not found: {key_path}\n"
                "   1. Go to demo.kalshi.co → Settings → API\n"
                "   2. Generate RSA key pair → download .pem\n"
                "   3. Set KALSHI_PRIVATE_KEY_PATH=./kalshi_private_key.pem in .env\n"
            )
        pem = open(key_path, "rb").read()
        try:
            self.private_key = serialization.load_pem_private_key(pem, password=None)
        except Exception as e:
            raise ValueError(f"❌ Cannot load private key from {key_path}: {e}")

        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        lbl = "🧪 DEMO" if self.mode == "demo" else "🔴 LIVE"
        logger.info(f"KalshiClient ready — {lbl} | {self.base_url}")

    # ── Signing ───────────────────────────────────────────────────────────────
    def _sign(self, method: str, endpoint: str) -> Dict[str, str]:
        """
        Build auth headers for a request.
        endpoint = just the resource path, e.g. "/portfolio/balance"
        Signing uses the FULL path: API_PREFIX + endpoint
        """
        ts_ms     = str(int(time.time() * 1000))
        full_path = API_PREFIX + endpoint          # /trade-api/v2/portfolio/balance
        message   = (ts_ms + method.upper() + full_path).encode()

        sig = self.private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY":       self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        }

    # ── HTTP Helpers ──────────────────────────────────────────────────────────
    def _get(self, ep: str, params: Optional[Dict] = None) -> Dict:
        r = self.session.get(self.base_url + ep,
                             headers=self._sign("GET", ep),
                             params=params, timeout=15)
        return self._handle(r)

    def _post(self, ep: str, body: Optional[Dict] = None) -> Dict:
        r = self.session.post(self.base_url + ep,
                              headers=self._sign("POST", ep),
                              json=body or {}, timeout=15)
        return self._handle(r)

    def _delete(self, ep: str) -> Dict:
        r = self.session.delete(self.base_url + ep,
                                headers=self._sign("DELETE", ep),
                                timeout=15)
        return self._handle(r)

    def _handle(self, resp: requests.Response) -> Dict:
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text}

        if not resp.ok:
            if isinstance(data, dict):
                msg = (data.get("error") or {}).get("message") \
                   or data.get("message") \
                   or data.get("error") \
                   or resp.text[:400]
            else:
                msg = resp.text[:400]
            raise KalshiAPIError(resp.status_code, str(msg))
        return data

    # ── Diagnostics ───────────────────────────────────────────────────────────
    def diagnose(self) -> Dict[str, Any]:
        """Full connectivity + auth check. Call this to debug connection issues."""
        result: Dict[str, Any] = {
            "mode": self.mode, "base_url": self.base_url,
            "key_id": self.key_id[:14] + "...",
            "host_reachable": False, "auth_ok": False,
            "balance_usd": None, "open_positions": None, "error": None,
        }
        # Host reachability
        host = DEMO_HOST if self.mode == "demo" else LIVE_HOST
        try:
            requests.get(host, timeout=6)
            result["host_reachable"] = True
        except Exception as e:
            result["error"] = f"Host unreachable: {e}"
            return result

        # Auth via balance
        try:
            bal = self.get_balance()
            result["auth_ok"]     = True
            result["balance_usd"] = bal.get("balance", 0) / 100.0
        except KalshiAPIError as e:
            result["error"] = f"Auth error [{e.status_code}]: {e.message}"
            if e.status_code == 401:
                result["error"] += (
                    "\n  ▶ Key ID or signature mismatch."
                    "\n  ▶ Demo keys only work on demo-api.kalshi.co."
                    "\n  ▶ Regenerate your API key in Kalshi settings."
                )
            elif e.status_code == 403:
                result["error"] += "\n  ▶ Key lacks trading permissions — enable in settings."
            return result
        except Exception as e:
            result["error"] = str(e)
            return result

        # Open positions
        try:
            pos = self.get_positions()
            result["open_positions"] = len(pos)
        except Exception:
            pass

        return result

    def print_diagnostics(self) -> bool:
        print("\n" + "═" * 58)
        print("  🔍  KALSHI CONNECTION DIAGNOSTICS")
        print("═" * 58)
        d = self.diagnose()
        print(f"  Mode:           {d['mode'].upper()}")
        print(f"  URL:            {d['base_url']}")
        print(f"  Key ID:         {d['key_id']}")
        print(f"  Host reachable: {'✅' if d['host_reachable'] else '❌'}")
        print(f"  Auth OK:        {'✅' if d['auth_ok'] else '❌'}")
        if d["balance_usd"] is not None:
            print(f"  Balance:        ${d['balance_usd']:,.2f}")
        if d["open_positions"] is not None:
            print(f"  Open positions: {d['open_positions']}")
        if d["error"]:
            print(f"\n  ❌  {d['error']}")
        else:
            print("\n  ✅  All checks passed — ready to trade!")
        print("═" * 58 + "\n")
        return d["auth_ok"]

    # ── Account ───────────────────────────────────────────────────────────────
    def get_balance(self) -> Dict:
        return self._get("/portfolio/balance")

    def get_balance_usd(self) -> float:
        return self.get_balance().get("balance", 0) / 100.0

    def get_positions(self) -> List[Dict]:
        return self._get("/portfolio/positions").get("market_positions", [])

    def get_fills(self, limit: int = 100) -> List[Dict]:
        return self._get("/portfolio/fills", {"limit": limit}).get("fills", [])

    def get_orders(self, status: str = "resting") -> List[Dict]:
        return self._get("/portfolio/orders", {"status": status}).get("orders", [])

    # ── Markets ───────────────────────────────────────────────────────────────
    def get_markets(self, status="open", limit=200, cursor=None,
                    series_ticker=None, min_close_ts=None, max_close_ts=None) -> Dict:
        p: Dict[str, Any] = {"status": status, "limit": limit}
        if cursor:        p["cursor"]        = cursor
        if series_ticker: p["series_ticker"] = series_ticker
        if min_close_ts:  p["min_close_ts"]  = min_close_ts
        if max_close_ts:  p["max_close_ts"]  = max_close_ts
        return self._get("/markets", params=p)

    def get_all_open_markets(self, max_close_hours: Optional[int] = None) -> List[Dict]:
        markets, cursor = [], None
        max_ts = int(time.time()) + max_close_hours * 3600 if max_close_hours else None
        while True:
            resp  = self.get_markets(cursor=cursor, max_close_ts=max_ts)
            batch = resp.get("markets", [])
            markets.extend(batch)
            cursor = resp.get("cursor")
            if not cursor or not batch:
                break
        logger.info(f"Fetched {len(markets)} open markets")
        return markets

    def get_market(self, ticker: str) -> Dict:
        return self._get(f"/markets/{ticker}").get("market", {})

    def get_orderbook(self, ticker: str, depth: int = 10) -> Dict:
        return self._get(f"/markets/{ticker}/orderbook", {"depth": depth})

    def extract_price_from_orderbook(self, ob: Dict) -> Dict:
        """
        Pull best yes_bid and yes_ask from a raw orderbook response.
        Kalshi orderbook format:
          { "orderbook": { "yes": [[price,qty],...], "no": [[price,qty],...] } }
        yes bids  = buyers of YES  -> highest yes bid
        yes asks  = sellers of YES -> derived from NO bids (100 - best_no_bid)
        """
        result = {}
        try:
            ob_data = ob.get("orderbook", ob)   # handle both wrapped and unwrapped
            yes_levels = ob_data.get("yes", [])   # [[price, qty], ...]
            no_levels  = ob_data.get("no",  [])

            # Best YES bid = highest price someone will BUY yes at
            if yes_levels:
                best_yes_bid = max(lvl[0] for lvl in yes_levels if len(lvl) >= 2)
                result["yes_bid"] = best_yes_bid

            # Best YES ask = 100 - best NO bid (someone selling YES = buying NO)
            if no_levels:
                best_no_bid = max(lvl[0] for lvl in no_levels if len(lvl) >= 2)
                result["yes_ask"] = 100 - best_no_bid

            # Mid price
            if "yes_bid" in result and "yes_ask" in result:
                result["last_price"] = (result["yes_bid"] + result["yes_ask"]) // 2

        except Exception as e:
            logger.debug(f"Orderbook parse error: {e}")

        return result

    def enrich_prices(self, markets: List[Dict],
                      max_enrich: int = 400,
                      delay_ms: float = 60) -> List[Dict]:
        """
        Fetch prices for markets that came back without bid/ask from the bulk endpoint.

        The Kalshi bulk /markets endpoint returns null prices for most markets.
        This method enriches them by fetching the individual market detail endpoint
        (GET /markets/{ticker}) which returns full price data including bid/ask/last.

        Strategy:
          1. Sort all unpriced markets by soonest close_time first
             (those are the live games you see in the Kalshi UI)
          2. Fetch up to max_enrich individual market details
          3. No OI/volume filter -- demo API returns 0 for both even on live games
        """
        already_priced = []
        need_price     = []

        for m in markets:
            has_price = (m.get("yes_bid") is not None or
                         m.get("yes_ask") is not None or
                         (m.get("last_price") is not None and m.get("last_price", -1) >= 0))
            if has_price:
                already_priced.append(m)
            else:
                need_price.append(m)

        if not need_price:
            logger.info(f"All {len(already_priced)} markets already have price data")
            return already_priced

        # Sort by soonest close_time = live/in-progress games first
        now = time.time()
        def sort_key(m):
            ct = m.get("close_time")
            if ct is None:
                return 9999999
            if isinstance(ct, str):
                try:
                    from datetime import datetime, timezone
                    dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                    return (dt.timestamp() - now)
                except Exception:
                    return 9999999
            return float(ct) - now

        need_price.sort(key=sort_key)

        enriched = []
        skipped  = 0

        logger.info(f"Enriching prices for up to {max_enrich} markets "
                    f"(sorted by closest close time)...")

        for i, m in enumerate(need_price):
            if len(enriched) >= max_enrich:
                skipped = len(need_price) - i
                break

            ticker = m.get("ticker", "")
            if not ticker:
                continue

            try:
                # Strategy A: fetch individual market detail (richer than bulk)
                detail = self.get_market(ticker)
                prices = {}

                for field in ("yes_bid", "yes_ask", "last_price", "last_yes_price"):
                    val = detail.get(field)
                    if val is not None:
                        prices[field] = val

                if not prices:
                    # Strategy B: try orderbook
                    ob     = self.get_orderbook(ticker, depth=5)
                    prices = self.extract_price_from_orderbook(ob)

                if prices:
                    merged = {**m, **detail, **prices}  # detail may have extra fields too
                    enriched.append(merged)
                # Whether we got prices or not, count as fetched
                time.sleep(delay_ms / 1000.0)

            except KalshiAPIError as e:
                logger.debug(f"Enrich API error {ticker} [{e.status_code}]: {e.message}")
                time.sleep(delay_ms / 1000.0)
            except Exception as e:
                logger.debug(f"Enrich error {ticker}: {e}")

        total = len(already_priced) + len(enriched)
        logger.info(
            f"Price enrichment complete: "
            f"{len(already_priced)} bulk-priced + "
            f"{len(enriched)} individually fetched + "
            f"{skipped} deferred (limit) = "
            f"{total} tradeable markets"
        )

        return already_priced + enriched

    def get_market_history(self, ticker: str, limit: int = 100) -> List[Dict]:
        return self._get(f"/markets/{ticker}/history", {"limit": limit}).get("history", [])

    def get_trades(self, ticker: str, limit: int = 50) -> List[Dict]:
        return self._get(f"/markets/{ticker}/trades", {"limit": limit}).get("trades", [])

    def get_events(self, status: str = "open", limit: int = 200) -> List[Dict]:
        return self._get("/events", {"status": status, "limit": limit}).get("events", [])

    # ── Orders ────────────────────────────────────────────────────────────────
    def place_order(self, ticker: str, side: str, action: str, count: int,
                    order_type: str = "limit",
                    yes_price: Optional[int] = None,
                    no_price:  Optional[int] = None,
                    client_order_id: Optional[str] = None) -> Dict:
        """
        Place a limit or market order.
        side:      "yes" | "no"
        action:    "buy" | "sell"
        yes_price / no_price: cents (1–99), omit for market order
        """
        body: Dict[str, Any] = {
            "ticker": ticker, "side": side,
            "action": action, "count": count, "type": order_type,
        }
        if yes_price is not None:  body["yes_price"] = yes_price
        if no_price  is not None:  body["no_price"]  = no_price
        if client_order_id:        body["client_order_id"] = client_order_id

        logger.info(f"📤 ORDER  {action} {count}x {side}@{ticker}  p={yes_price or no_price}¢")
        return self._post("/portfolio/orders", body)

    def cancel_order(self, order_id: str) -> Dict:
        return self._delete(f"/portfolio/orders/{order_id}")

    def cancel_all_orders(self) -> int:
        n = 0
        for o in self.get_orders():
            try:
                self.cancel_order(o["order_id"]); n += 1
            except Exception as e:
                logger.warning(f"Cancel failed {o['order_id']}: {e}")
        return n

    # ── Utilities ─────────────────────────────────────────────────────────────
    @staticmethod
    def cents_to_usd(c: int) -> float:        return c / 100.0
    @staticmethod
    def usd_to_cents(u: float) -> int:        return int(round(u * 100))
    @staticmethod
    def implied_prob(yes_cents: int) -> float: return yes_cents / 100.0

    def ping(self) -> bool:
        try:
            return "balance" in self.get_balance()
        except Exception:
            return False