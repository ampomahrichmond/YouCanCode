"""
strategies.py  v3
=================
All 7 trading strategies for Kalshi prediction markets.

FIXES vs v2 (real-data compatibility):
  1. _mid_price: last_price=0 is valid (1-cent market), not missing data
  2. _volume:    uses volume_24h first (more relevant), falls back to volume/OI
  3. _category:  massively expanded to cover real Kalshi series naming
  4. _hours_left: handles extra field names (expected_expiration_time)
  5. NoSideBias: MAX_HOURS raised 24->72, MIN_VOLUME lowered 200->50
  6. MarketMaking: MIN_SPREAD lowered 6->4, MIN_VOLUME lowered 500->100
  7. StinkBid:   MIN_VOLUME lowered 150->30
  8. audit_market() helper for --debug diagnostics

Strategies:
  1. ValueEdge     -- price vs EMA fair value (finance/crypto/weather)
  2. NoSideBias    -- fade public YES overbetting in sports
  3. Momentum      -- enter in direction of strong recent price move
  4. MarketMaking  -- capture bid/ask spread (0-fee Kalshi)
  5. StatArb       -- cross-market price inconsistencies
  6. StinkBid      -- limit buy way below market on longshots
  7. TimeDecay     -- near-expiry convergence theta play

Author: APEX Trading Suite - Kalshi Module
"""

import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("Strategies")


# =============================================================================
# Signal Types
# =============================================================================
class SignalType(Enum):
    BUY_YES       = "buy_yes"
    BUY_NO        = "buy_no"
    SELL_YES      = "sell_yes"
    SELL_NO       = "sell_no"
    MAKE_MARKET   = "make_market"
    STINK_BID_YES = "stink_bid_yes"
    STINK_BID_NO  = "stink_bid_no"
    HOLD          = "hold"

# Alias for backward compat
SignalDirection = SignalType


@dataclass
class TradeSignal:
    ticker:               str
    direction:            SignalType
    estimated_probability:float
    market_price_yes:     float
    edge:                 float
    confidence:           float
    strategy_name:        str
    reasoning:            str
    market_title:         str = ""
    category:             str = ""
    close_ts:             int = 0
    mm_bid_yes:           Optional[int] = None
    mm_ask_yes:           Optional[int] = None
    mm_contracts:         int           = 0
    stink_price_cents:    Optional[int] = None
    stink_side:           str           = ""

    @property
    def ev(self) -> float:
        return self.edge * self.confidence


# =============================================================================
# Shared Helpers  (FIX: all 5 real-data bugs fixed here)
# =============================================================================

def _mid_price(market: Dict) -> Optional[float]:
    """
    YES mid-price as 0.0-1.0.
    FIX: last_price=0 is a valid 0-cent price, NOT missing data.
         Use explicit `is not None` checks throughout.
    """
    bid = market.get("yes_bid")
    ask = market.get("yes_ask")

    if bid is not None and ask is not None and ask > 0:
        return (bid + ask) / 2.0 / 100.0
    if bid is not None and bid > 0:
        return bid / 100.0
    if ask is not None and ask > 0:
        return ask / 100.0

    # FIX: explicit None check -- price=0 (1c) IS valid
    for field in ("last_price", "last_yes_price"):
        lp = market.get(field)
        if lp is not None:
            return lp / 100.0

    return None


def _spread_cents(market: Dict) -> int:
    """YES ask minus YES bid in cents. 0 if no bid/ask."""
    bid = market.get("yes_bid")
    ask = market.get("yes_ask")
    if bid is None or ask is None:
        return 0
    return max(0, ask - bid)


def _hours_left(market: Dict) -> float:
    """
    Hours until market close.
    FIX: also checks expected_expiration_time used by some real Kalshi markets.
    """
    ct = (market.get("close_time")
          or market.get("expiration_time")
          or market.get("expected_expiration_time"))
    if ct is None:
        return 999.0
    if isinstance(ct, str):
        try:
            dt  = datetime.fromisoformat(ct.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            return max(0.0, (dt - now).total_seconds() / 3600.0)
        except Exception:
            return 999.0
    return max(0.0, (float(ct) - time.time()) / 3600.0)


def _volume(market: Dict) -> int:
    """
    Best-effort volume.
    FIX: real Kalshi markets have tiny lifetime `volume` for new markets.
    Prefer volume_24h (last 24h activity), then volume, then open_interest.
    """
    for field in ("volume_24h", "volume", "open_interest"):
        v = market.get(field)
        if v is not None and v > 0:
            return int(v)
    return 0


def _category(market: Dict) -> str:
    """
    Classify a Kalshi market.
    FIX: greatly expanded to cover real Kalshi series names and title patterns.
    """
    s = market.get("series_ticker", "").upper()
    e = market.get("event_ticker", "").upper()
    t = market.get("title", "").lower()

    # --- Sports (check series/event ticker first) ---
    SPORTS_SERIES = {
        # Major US leagues
        "NFL","NBA","MLB","NHL","NCAAB","NCAAF","NCAAW","MLS","WNBA","NWSL",
        # Kalshi KX prefixed
        "KXNBA","KXNFL","KXMLB","KXNHL","KXNCAAB","KXNCAAF","KXMLS","KXWNBA",
        # Game-specific series names Kalshi uses
        "NFLGAME","NBAGAME","MLBGAME","NHLGAME","NFLWK","NBAWK","NFLPLAYOFF",
        "NBAPLAYOFF","MLBPLAYOFF","NHLPLAYOFF","NFLSB","SUPERBOWL",
        "WORLDSERIES","STANLEYCUP","NBAFINALS","NBACONF","NLCSCHAMP","ALCHAMP",
        # College
        "MARCHMADNESS","NCAATOURNAMENT","CFBCHAMP","CFBPLAYOFF","BOWLGAME",
        # Soccer
        "SOCCER","EPL","EPLGAME","UEFA","UEFACL","LALIGA","BUNDESLIGA",
        "SERIEA","LIGUE1","MLS","MLSGAME","USMNT","USWNT","WORLDCUP","EURO",
        "COPAAMERICA","CONCACAF","FIFAWC",
        # Other sports
        "TENNIS","ATP","WTA","WIMBLEDON","USOPEN","FRENCHOPEN","AUSOPEN",
        "PGA","PGAGOLF","MASTERS","USOPENGOLF","GOLF","LPGA",
        "UFC","MMA","BOXING","BOXING","F1","NASCAR","INDYCAR","FORMULA",
        # Generic
        "CBBT","CFBT","SPORTSPROP","PLAYERPROP",
    }
    if any(k in s for k in SPORTS_SERIES) or any(k in e for k in SPORTS_SERIES):
        return "sports"

    # Sports via title keywords
    SPORTS_TITLE = [
        " win ", "wins the", "wins tonight", "wins today", "win tonight",
        "win today", "cover the spread", "cover -", "over/under",
        "score more than", "beat the", " ml ", "moneyline",
        "super bowl", "world series", "stanley cup", "nba finals",
        "championship game", "playoff", "tournament",
        "first round", "second round", "final four",
        "touchdown", "home run", "goal scorer", "hat trick",
        "premier league", "champions league", "bundesliga", "serie a",
        "la liga", "ufc ", "boxing match", "formula 1", "grand prix",
    ]
    if any(k in t for k in SPORTS_TITLE):
        return "sports"

    # --- Crypto ---
    CRYPTO_SERIES = {"KXBTC","KXETH","KXSOL","KXDOGE","KXXRP","KXBNB","KXADA"}
    CRYPTO_TITLE  = [
        "bitcoin","btc ","btc-","eth ","ethereum","crypto","solana","sol ",
        "dogecoin","doge","xrp","ripple","bnb ","matic","polygon","chainlink",
        "litecoin","ltc","cardano","ada","avalanche","avax","shiba","pepe",
        " coin "," token ","defi","nft",
    ]
    if any(k in s for k in CRYPTO_SERIES) or any(k in t for k in CRYPTO_TITLE):
        return "crypto"

    # --- Finance / Macro ---
    FINANCE_SERIES = {
        "INXCHI","INX","KXFED","KXCPI","KXUNEMP","KXGDP","KXOIL",
        "KXGOLD","KXNASDAQ","KXDOW","KXSP","KXSPX","KXVIX","KXRATES",
        "KXJOBS","KXHOUSING","KXRETAIL","KXINFL",
    }
    FINANCE_TITLE = [
        "s&p 500","s&p500","sp 500","nasdaq","dow jones","dow ",
        "federal reserve","fed funds","interest rate","rate cut","rate hike",
        "cpi report","inflation","core pce","gdp","recession","unemployment",
        "payrolls","nonfarm","jobs report","ism","pmi","retail sales",
        "10-year yield","treasury","wti oil","brent crude","natural gas",
        "gold price","silver price","dollar index","dxy","eur/usd","usd/jpy",
        "stock market","market crash","market rally","earnings",
    ]
    if any(k in s for k in FINANCE_SERIES) or any(k in t for k in FINANCE_TITLE):
        return "finance"

    # --- Weather ---
    WEATHER_TITLE = [
        "hurricane","tropical storm","typhoon","tornado","earthquake",
        "temperature above","temperature below","high of","low of",
        "degrees fahrenheit","degrees celsius","snow accumulation",
        "inches of snow","inches of rain","rainfall","precipitation",
        "flooding","wildfire","heat wave","cold snap","blizzard",
        "named storm","atlantic storm",
    ]
    if any(k in t for k in WEATHER_TITLE) or "KXWEATHER" in s:
        return "weather"

    # --- Politics ---
    POLITICS_TITLE = [
        "election","president","senate","congress","house of representatives",
        "governor","primary","ballot measure","popular vote","electoral",
        "approval rating","impeach","resign","nominee","supreme court",
        "legislation pass","executive order","veto","filibuster",
    ]
    if any(k in t for k in POLITICS_TITLE):
        return "politics"

    return "other"


def _ema(values: List[float], alpha: float = 0.3) -> float:
    if not values:
        return 0.0
    ema = values[0]
    for v in values[1:]:
        ema = alpha * v + (1 - alpha) * ema
    return ema


def audit_market(market: Dict) -> Dict:
    """
    Debug helper -- returns a summary dict for why a market might get no signals.
    Call this from --debug mode to diagnose zero-signal situations.
    """
    yes    = _mid_price(market)
    hours  = _hours_left(market)
    vol    = _volume(market)
    cat    = _category(market)
    spread = _spread_cents(market)
    return {
        "ticker":     market.get("ticker", ""),
        "title":      market.get("title", "")[:55],
        "category":   cat,
        "yes_price":  f"{yes:.0%}" if yes is not None else "NO_PRICE",
        "hours_left": f"{hours:.1f}h",
        "volume":     vol,
        "spread":     f"{spread}c",
        "yes_bid":    market.get("yes_bid"),
        "yes_ask":    market.get("yes_ask"),
        "last_price": market.get("last_price"),
        "series":     market.get("series_ticker", ""),
        "status":     market.get("status", ""),
    }


# =============================================================================
# Strategy 1 -- Value Edge
# =============================================================================
class ValueEdgeStrategy:
    """
    Finds mispriced finance/crypto/weather markets using EMA fair-value.
    Requires price history to compute EMA; without it uses current price as fair value.
    MIN_EDGE lowered to 0.05 for real-market compatibility.
    """
    name     = "ValueEdge"
    MIN_EDGE = 0.05
    MIN_CONF = 0.48

    def analyze(self, market: Dict, history: Optional[List] = None) -> Optional[TradeSignal]:
        if _category(market) == "sports":
            return None
        yes = _mid_price(market)
        if yes is None or not (0.04 < yes < 0.96):
            return None
        hours = _hours_left(market)
        if hours < 0.25 or hours > 120:
            return None
        spread = _spread_cents(market)
        if spread > 18:
            return None

        fair = yes
        if history and len(history) >= 5:
            prices = [h["yes_price"] / 100.0 for h in history[-12:]
                      if h.get("yes_price") is not None]
            if len(prices) >= 5:
                fair = _ema(prices)

        edge_yes = fair - yes
        edge_no  = (1 - fair) - (1 - yes)
        best_edge = max(abs(edge_yes), abs(edge_no))
        if best_edge < self.MIN_EDGE:
            return None

        vol  = _volume(market)
        conf = min(0.90, (best_edge / 0.20) * 0.5 + min(vol, 500) / 1000 * 0.5)
        if conf < self.MIN_CONF:
            return None

        if edge_yes > 0:
            d, ep, e = SignalType.BUY_YES, fair, edge_yes
            reason = f"YES cheap: market={yes:.0%} vs EMA={fair:.0%} edge={edge_yes:.0%}"
        else:
            d, ep, e = SignalType.BUY_NO, 1 - fair, abs(edge_no)
            reason = f"NO cheap: mkt_NO={1-yes:.0%} vs EMA_NO={1-fair:.0%} edge={abs(edge_no):.0%}"

        return TradeSignal(
            ticker=market.get("ticker",""), direction=d,
            estimated_probability=ep, market_price_yes=yes,
            edge=e, confidence=conf, strategy_name=self.name, reasoning=reason,
            market_title=market.get("title",""),
            category=_category(market), close_ts=market.get("close_time",0),
        )


# =============================================================================
# Strategy 2 -- NO-Side Public Bias (Sports)
# =============================================================================
class NoSideBiasStrategy:
    """
    Fades systematic YES-side overbetting in sports prediction markets.

    FIX: MAX_HOURS raised from 24 to 72 to catch pre-game markets.
         MIN_VOLUME lowered from 200 to 50 to catch real markets.
         STRONG_SERIES expanded with real Kalshi series names.
    """
    name          = "NoSideBias"
    NO_FLOOR      = 0.55
    NO_CEIL       = 0.84
    MIN_VOLUME    = 50        # FIX: was 200, real markets often have lower volume
    MIN_HOURS     = 0.25
    MAX_HOURS     = 72.0      # FIX: was 24, now catches pre-game markets
    STRONG_SERIES = {
        "KXNFL","KXNBA","KXMLB","KXNHL","KXNCAAB","NFL","NBA","MLB","NHL",
        "NCAAB","NFLGAME","NBAGAME","MLBGAME","NHLGAME","NFLWK",
        "NFLPLAYOFF","NBAPLAYOFF","MLBPLAYOFF","NHLPLAYOFF",
    }

    def analyze(self, market: Dict) -> Optional[TradeSignal]:
        if _category(market) != "sports":
            return None
        yes = _mid_price(market)
        if yes is None:
            return None
        no_imp = 1.0 - yes
        if not (self.NO_FLOOR <= no_imp <= self.NO_CEIL):
            return None
        hours = _hours_left(market)
        if not (self.MIN_HOURS <= hours <= self.MAX_HOURS):
            return None
        vol = _volume(market)
        if vol < self.MIN_VOLUME:
            return None

        if no_imp >= 0.72:   bias = 0.07
        elif no_imp >= 0.63: bias = 0.06
        else:                bias = 0.05

        est_no = no_imp + bias
        series = market.get("series_ticker","").upper()
        strong = any(s in series for s in self.STRONG_SERIES)
        conf = 0.55 + (0.15 if strong else 0.0) + min(0.30, vol / 500 * 0.30)

        return TradeSignal(
            ticker=market.get("ticker",""), direction=SignalType.BUY_NO,
            estimated_probability=est_no, market_price_yes=yes,
            edge=bias, confidence=min(conf, 0.95), strategy_name=self.name,
            reasoning=(f"Sports NO bias: YES={yes:.0%} NO={no_imp:.0%} "
                       f"+{bias:.0%} correction -> est_NO={est_no:.0%}"),
            market_title=market.get("title",""), category="sports",
            close_ts=market.get("close_time",0),
        )


# =============================================================================
# Strategy 3 -- Momentum
# =============================================================================
class MomentumStrategy:
    """
    Enters in direction of strong price move near market close.
    Requires history data; returns None without it.
    """
    name       = "Momentum"
    MIN_MOVE   = 0.08
    LOOKBACK   = 5
    MIN_HOURS  = 0.25
    MAX_HOURS  = 6.0
    MIN_VOLUME = 100   # FIX: was 300

    def analyze(self, market: Dict, history: List) -> Optional[TradeSignal]:
        if len(history) < self.LOOKBACK:
            return None
        yes = _mid_price(market)
        if yes is None or not (0.06 < yes < 0.94):
            return None
        hours = _hours_left(market)
        if not (self.MIN_HOURS <= hours <= self.MAX_HOURS):
            return None
        if _volume(market) < self.MIN_VOLUME:
            return None

        prices = [h["yes_price"] / 100.0 for h in history
                  if h.get("yes_price") is not None]
        if len(prices) < self.LOOKBACK:
            return None

        momentum = prices[-1] - prices[-self.LOOKBACK]
        if abs(momentum) < self.MIN_MOVE:
            return None

        boost = min(0.06, abs(momentum) * 0.4)
        conf  = min(0.80, 0.50 + abs(momentum) * 2.0)
        d = SignalType.BUY_YES if momentum > 0 else SignalType.BUY_NO
        ep = (yes + boost) if momentum > 0 else ((1 - yes) + boost)

        return TradeSignal(
            ticker=market.get("ticker",""), direction=d,
            estimated_probability=ep, market_price_yes=yes,
            edge=boost, confidence=conf, strategy_name=self.name,
            reasoning=f"Momentum: {'+' if momentum>0 else ''}{momentum:.0%} over {self.LOOKBACK}p, {hours:.1f}h left",
            market_title=market.get("title",""),
            category=_category(market), close_ts=market.get("close_time",0),
        )


# =============================================================================
# Strategy 4 -- Market Making (Spread Capture)
# =============================================================================
class MarketMakingStrategy:
    """
    Post limit orders on both sides of a wide-spread market.
    With 0% fees on Kalshi, the entire spread is profit.

    FIX: MIN_SPREAD lowered 6->4 cents, MIN_VOLUME lowered 500->100.
         Real Kalshi liquid markets have 2-6 cent spreads.
    """
    name             = "MarketMaking"
    MIN_SPREAD       = 4       # FIX: was 6
    MAX_SPREAD       = 22
    MIN_VOLUME       = 100     # FIX: was 500
    MIN_OI           = 50      # FIX: was 200
    MIN_HOURS        = 1.0
    MAX_HOURS        = 72.0
    MM_OFFSET        = 1       # FIX: was 2, tighter for real markets
    BANNED_CAT       = {"politics"}

    def analyze(self, market: Dict, orderbook: Optional[Dict] = None) -> Optional[TradeSignal]:
        cat = _category(market)
        if cat in self.BANNED_CAT:
            return None
        yes_bid = market.get("yes_bid")
        yes_ask = market.get("yes_ask")
        if yes_bid is None or yes_ask is None:
            return None
        spread = yes_ask - yes_bid
        if not (self.MIN_SPREAD <= spread <= self.MAX_SPREAD):
            return None
        hours = _hours_left(market)
        if not (self.MIN_HOURS <= hours <= self.MAX_HOURS):
            return None
        vol = _volume(market)
        oi  = market.get("open_interest", 0) or 0
        if vol < self.MIN_VOLUME:
            return None

        our_yes_bid = yes_bid + self.MM_OFFSET
        our_yes_ask = yes_ask - self.MM_OFFSET
        if our_yes_bid >= our_yes_ask:
            return None
        if not (1 <= our_yes_bid <= 98 and 2 <= our_yes_ask <= 99):
            return None

        captured = our_yes_ask - our_yes_bid
        edge     = captured / 100.0 / 2.0
        liq_score = min(1.0, vol / 1000.0)
        spd_score = 1.0 - (spread - self.MIN_SPREAD) / max(1, self.MAX_SPREAD - self.MIN_SPREAD)
        confidence = 0.55 + 0.25 * liq_score + 0.20 * spd_score
        yes_mid = (yes_bid + yes_ask) / 2 / 100.0

        return TradeSignal(
            ticker=market.get("ticker",""), direction=SignalType.MAKE_MARKET,
            estimated_probability=yes_mid, market_price_yes=yes_mid,
            edge=edge, confidence=confidence, strategy_name=self.name,
            reasoning=f"MM: spread={spread}c  post {our_yes_bid}/{our_yes_ask}  capture={captured}c  vol={vol}",
            market_title=market.get("title",""), category=cat,
            close_ts=market.get("close_time",0),
            mm_bid_yes=our_yes_bid, mm_ask_yes=our_yes_ask, mm_contracts=5,
        )


# =============================================================================
# Strategy 5 -- Statistical Arbitrage
# =============================================================================
class StatArbStrategy:
    """
    Cross-market price inconsistencies.
    TYPE A: Threshold ladder inversion within same series.
    TYPE B: Head-to-head probability sum != 1.0

    FIX: groups markets by event_ticker OR series_ticker for real Kalshi data.
    """
    name         = "StatArb"
    MIN_ARB_EDGE = 0.04
    MIN_VOLUME   = 30    # FIX: was 200
    MIN_HOURS    = 0.5

    def scan_for_arb(self, markets: List[Dict]) -> List[TradeSignal]:
        signals: List[TradeSignal] = []

        # Build index -- prefer event_ticker (more specific), fall back to series_ticker
        by_event: Dict[str, List[Dict]] = {}
        by_series: Dict[str, List[Dict]] = {}
        for m in markets:
            et = m.get("event_ticker","")
            st = m.get("series_ticker","")
            if et:
                by_event.setdefault(et, []).append(m)
            if st:
                by_series.setdefault(st, []).append(m)

        # TYPE A: Ladder arb within same series
        for series, series_markets in by_series.items():
            signals.extend(self._ladder_arb(series, series_markets))

        # TYPE B: H2H sum arb within same event
        for event_id, event_markets in by_event.items():
            signals.extend(self._h2h_sum_arb(event_id, event_markets))

        return signals

    def _ladder_arb(self, series: str, markets: List[Dict]) -> List[TradeSignal]:
        signals = []
        if len(markets) < 2:
            return signals

        valid = []
        for m in markets:
            p = _mid_price(m)
            h = _hours_left(m)
            v = _volume(m)
            if p is not None and h >= self.MIN_HOURS and v >= self.MIN_VOLUME:
                valid.append((m, p))

        if len(valid) < 2:
            return signals

        valid.sort(key=lambda x: x[1], reverse=True)

        for i in range(len(valid) - 1):
            m_hi, p_hi = valid[i]
            m_lo, p_lo = valid[i + 1]
            inversion = p_lo - p_hi
            if inversion > self.MIN_ARB_EDGE:
                edge = inversion / 2
                conf = min(0.85, 0.60 + inversion * 3)
                signals.append(TradeSignal(
                    ticker=m_lo.get("ticker",""), direction=SignalType.BUY_NO,
                    estimated_probability=1 - p_lo + edge, market_price_yes=p_lo,
                    edge=edge, confidence=conf, strategy_name=self.name,
                    reasoning=(f"LADDER ARB [{series}]: {m_lo.get('ticker')} YES={p_lo:.0%} "
                               f"> {m_hi.get('ticker')} YES={p_hi:.0%}  inversion={inversion:.0%}"),
                    market_title=m_lo.get("title",""),
                    category=_category(m_lo), close_ts=m_lo.get("close_time",0),
                ))
        return signals

    def _h2h_sum_arb(self, event_id: str, markets: List[Dict]) -> List[TradeSignal]:
        signals = []
        valid = [(m, _mid_price(m)) for m in markets
                 if _mid_price(m) is not None
                 and _hours_left(m) >= self.MIN_HOURS
                 and _volume(m) >= self.MIN_VOLUME]

        if len(valid) < 2:
            return signals

        for i, (m1, p1) in enumerate(valid):
            for m2, p2 in valid[i+1:]:
                total = p1 + p2
                if total < 0.93:
                    gap  = 1.0 - total
                    edge = gap / 2.0
                    if edge >= self.MIN_ARB_EDGE:
                        conf = min(0.80, 0.55 + gap * 2)
                        for m, p in [(m1,p1),(m2,p2)]:
                            signals.append(TradeSignal(
                                ticker=m.get("ticker",""), direction=SignalType.BUY_YES,
                                estimated_probability=p + edge, market_price_yes=p,
                                edge=edge, confidence=conf, strategy_name=self.name,
                                reasoning=(f"H2H ARB: {m1.get('ticker')} P={p1:.0%} + "
                                           f"{m2.get('ticker')} P={p2:.0%} = {total:.0%} < 1.0  edge={edge:.0%}"),
                                market_title=m.get("title",""),
                                category=_category(m), close_ts=m.get("close_time",0),
                            ))
                elif total > 1.07:
                    gap  = total - 1.0
                    edge = gap / 2.0
                    if edge >= self.MIN_ARB_EDGE:
                        conf = min(0.80, 0.55 + gap * 2)
                        for m, p in [(m1,p1),(m2,p2)]:
                            signals.append(TradeSignal(
                                ticker=m.get("ticker",""), direction=SignalType.BUY_NO,
                                estimated_probability=(1-p) + edge, market_price_yes=p,
                                edge=edge, confidence=conf, strategy_name=self.name,
                                reasoning=(f"H2H ARB: {m1.get('ticker')} P={p1:.0%} + "
                                           f"{m2.get('ticker')} P={p2:.0%} = {total:.0%} > 1.0  edge={edge:.0%}"),
                                market_title=m.get("title",""),
                                category=_category(m), close_ts=m.get("close_time",0),
                            ))
        return signals


# =============================================================================
# Strategy 6 -- Stink Bids
# =============================================================================
class StinkBidStrategy:
    """
    Limit buy far below market on longshot markets.
    FIX: MIN_VOLUME lowered to 30 for real markets.
    """
    name          = "StinkBid"
    YES_MIN       = 0.03
    YES_MAX       = 0.15
    DISCOUNT_PCT  = 0.50
    MIN_BID_CENTS = 1
    MAX_BID_CENTS = 5
    MIN_VOLUME    = 30     # FIX: was 150
    MIN_HOURS     = 2.0
    MAX_HOURS     = 120.0
    ALLOWED_CAT   = {"finance", "crypto", "weather"}

    def analyze(self, market: Dict) -> Optional[TradeSignal]:
        cat = _category(market)
        if cat not in self.ALLOWED_CAT:
            return None
        yes = _mid_price(market)
        if yes is None or not (self.YES_MIN <= yes <= self.YES_MAX):
            return None
        hours = _hours_left(market)
        if not (self.MIN_HOURS <= hours <= self.MAX_HOURS):
            return None
        if _volume(market) < self.MIN_VOLUME:
            return None

        yes_bid    = market.get("yes_bid", int(yes * 100))
        stink_cents= max(self.MIN_BID_CENTS,
                         min(self.MAX_BID_CENTS, int(yes_bid * (1 - self.DISCOUNT_PCT))))
        if stink_cents < 1:
            return None

        ev_mult = yes / (stink_cents / 100.0)
        if ev_mult < 1.30:
            return None

        edge = yes * 1.0 - stink_cents / 100.0
        conf = min(0.70, 0.40 + (ev_mult - 1.3) * 0.2)

        return TradeSignal(
            ticker=market.get("ticker",""), direction=SignalType.STINK_BID_YES,
            estimated_probability=yes, market_price_yes=yes,
            edge=edge, confidence=conf, strategy_name=self.name,
            reasoning=(f"Stink bid: market={yes_bid}c  limit@{stink_cents}c "
                       f"(-{(1-stink_cents/max(1,yes_bid))*100:.0f}%)  EV_mult={ev_mult:.2f}x"),
            market_title=market.get("title",""), category=cat,
            close_ts=market.get("close_time",0),
            stink_price_cents=stink_cents, stink_side="yes",
        )


# =============================================================================
# Strategy 7 -- Time Decay (Theta)
# =============================================================================
class TimeDecayStrategy:
    """
    Capture residual uncertainty premium in near-expiry high-confidence markets.
    Sub-play A: buy YES when market is 80-97% with < 4h left.
    Sub-play B: buy NO when market is 3-20% with < 3h left.
    """
    name       = "TimeDecay"
    YES_FLOOR_A = 0.80
    YES_CEIL_A  = 0.97
    MAX_HOURS_A = 4.0
    MIN_HOURS_A = 0.1
    YES_CEIL_B  = 0.20
    YES_FLOOR_B = 0.03
    MAX_HOURS_B = 3.0
    MIN_HOURS_B = 0.1
    MIN_VOLUME  = 50   # FIX: was 200
    BANNED_CAT  = {"politics"}

    def analyze(self, market: Dict) -> Optional[TradeSignal]:
        cat = _category(market)
        if cat in self.BANNED_CAT:
            return None
        yes   = _mid_price(market)
        hours = _hours_left(market)
        if yes is None or _volume(market) < self.MIN_VOLUME:
            return None

        # Sub-play A: near-certain YES
        if self.YES_FLOOR_A <= yes <= self.YES_CEIL_A:
            if not (self.MIN_HOURS_A <= hours <= self.MAX_HOURS_A):
                return None
            no_premium = 1.0 - yes
            time_factor = 1.0 - (hours / self.MAX_HOURS_A)
            conf = min(0.92, yes * time_factor * 1.1)
            if conf < 0.60:
                return None
            return TradeSignal(
                ticker=market.get("ticker",""), direction=SignalType.BUY_YES,
                estimated_probability=yes + no_premium * 0.85,
                market_price_yes=yes, edge=no_premium * 0.85,
                confidence=conf, strategy_name=self.name,
                reasoning=f"THETA-A: YES={yes:.0%} residual_NO={no_premium:.0%} {hours:.2f}h left",
                market_title=market.get("title",""), category=cat,
                close_ts=market.get("close_time",0),
            )

        # Sub-play B: near-certain NO
        if self.YES_FLOOR_B <= yes <= self.YES_CEIL_B:
            if not (self.MIN_HOURS_B <= hours <= self.MAX_HOURS_B):
                return None
            yes_premium = yes
            time_factor = 1.0 - (hours / self.MAX_HOURS_B)
            conf = min(0.92, (1-yes) * time_factor * 1.1)
            if conf < 0.60:
                return None
            return TradeSignal(
                ticker=market.get("ticker",""), direction=SignalType.BUY_NO,
                estimated_probability=(1-yes) + yes * 0.85,
                market_price_yes=yes, edge=yes_premium * 0.85,
                confidence=conf, strategy_name=self.name,
                reasoning=f"THETA-B: YES={yes:.0%} residual_YES={yes_premium:.0%} {hours:.2f}h left",
                market_title=market.get("title",""), category=cat,
                close_ts=market.get("close_time",0),
            )

        return None


# =============================================================================
# Strategy Engine
# =============================================================================
class StrategyEngine:
    """
    Orchestrates all 7 strategies.
    scan_all() returns signals sorted by EV (edge x confidence).
    """

    def __init__(self, min_edge: float = 0.04, min_confidence: float = 0.48):
        self.min_edge       = min_edge
        self.min_confidence = min_confidence
        self.value_edge   = ValueEdgeStrategy()
        self.no_side      = NoSideBiasStrategy()
        self.momentum     = MomentumStrategy()
        self.market_maker = MarketMakingStrategy()
        self.stat_arb     = StatArbStrategy()
        self.stink_bid    = StinkBidStrategy()
        self.time_decay   = TimeDecayStrategy()

    def scan_market(self, market: Dict,
                    history: Optional[List] = None,
                    orderbook: Optional[Dict] = None,
                    enabled: Optional[List[str]] = None) -> List[TradeSignal]:
        if enabled is None:
            enabled = ["value_edge","no_side","momentum",
                       "market_maker","stink_bid","time_decay"]
        signals = []
        try:
            if "value_edge"   in enabled:
                s = self.value_edge.analyze(market, history)
                if s: signals.append(s)
            if "no_side"      in enabled:
                s = self.no_side.analyze(market)
                if s: signals.append(s)
            if "momentum"     in enabled:
                s = self.momentum.analyze(market, history or [])
                if s: signals.append(s)
            if "market_maker" in enabled:
                s = self.market_maker.analyze(market, orderbook)
                if s: signals.append(s)
            if "stink_bid"    in enabled:
                s = self.stink_bid.analyze(market)
                if s: signals.append(s)
            if "time_decay"   in enabled:
                s = self.time_decay.analyze(market)
                if s: signals.append(s)
        except Exception as ex:
            logger.debug(f"Strategy error on {market.get('ticker')}: {ex}")

        return [s for s in signals
                if s.edge >= self.min_edge and s.confidence >= self.min_confidence]

    def scan_all(self, markets: List[Dict],
                 histories: Optional[Dict[str, List]] = None,
                 orderbooks: Optional[Dict[str, Dict]] = None,
                 enabled: Optional[List[str]] = None) -> List[TradeSignal]:
        all_sigs: List[TradeSignal] = []

        for m in markets:
            ticker = m.get("ticker","")
            hist   = (histories  or {}).get(ticker)
            ob     = (orderbooks or {}).get(ticker)
            all_sigs.extend(self.scan_market(m, hist, ob, enabled))

        # StatArb runs across full market list
        if enabled is None or "stat_arb" in (enabled or []):
            try:
                arb = self.stat_arb.scan_for_arb(markets)
                all_sigs.extend(
                    s for s in arb
                    if s.edge >= self.min_edge and s.confidence >= self.min_confidence
                )
            except Exception as ex:
                logger.debug(f"StatArb error: {ex}")

        all_sigs.sort(key=lambda s: s.ev, reverse=True)
        return all_sigs

    def debug_scan(self, markets: List[Dict], sample: int = 30) -> str:
        """
        Return a human-readable breakdown of why markets are getting filtered.
        Call this when you see 'No qualifying signals'.
        """
        from collections import Counter
        cats    = Counter()
        no_price= 0
        no_vol  = 0
        out_hours = 0
        wrong_range = 0
        passed  = 0

        for m in markets[:min(len(markets), 5000)]:
            p = _mid_price(m)
            h = _hours_left(m)
            v = _volume(m)
            c = _category(m)
            cats[c] += 1
            if p is None:             no_price  += 1
            elif v < 30:              no_vol    += 1
            elif h > 120 or h < 0.1:  out_hours += 1
            elif not (0.04 < p < 0.96): wrong_range += 1
            else:                     passed    += 1

        lines = [
            f"  Debug scan ({min(len(markets),5000)} of {len(markets)} markets):",
            f"  Categories: {dict(cats)}",
            f"  No price data:      {no_price}",
            f"  Volume < 30:        {no_vol}",
            f"  Hours out of range: {out_hours}",
            f"  Price out of range: {wrong_range}",
            f"  Passed pre-filters: {passed}",
        ]

        # Sample 5 real markets to show actual field values
        lines.append("  Sample markets (first 5):")
        for m in markets[:5]:
            a = audit_market(m)
            lines.append(
                f"    {a['ticker'][:30]:32} cat={a['category']:8} "
                f"yes={a['yes_price']:7} vol={a['volume']:5} "
                f"hrs={a['hours_left']:7} spread={a['spread']:4} "
                f"bid={a['yes_bid']} ask={a['yes_ask']} lp={a['last_price']}"
            )
        return "\n".join(lines)