"""
hedge_manager.py — Delta-Neutral Perpetual Futures Hedge Manager

Eliminates inventory price risk from pre-staged crypto positions.

The Core Idea:
  When you hold ETH spot on Bybit (waiting to sell in an arb),
  you open an equal-size SHORT on Bybit's USDT-M Perpetual futures.
  Price goes up → spot gains, perp loses (cancel out).
  Price goes down → spot loses, perp gains (cancel out).
  Net directional exposure = 0. You only profit from the arb spread.

Lifecycle Managed Here:
  1. open_hedge(symbol, qty)  → SHORT perp when restaging crypto on Bybit
  2. close_hedge(symbol, qty) → CLOSE SHORT when arb fires (spot sold)
  3. get_funding_cost(symbol) → Accrued funding rate cost for profit calc
  4. get_funding_rate(symbol) → Live funding rate (fetch before each trade)

Bybit USDT-M Perp API specifics:
  - category = "linear"
  - Uses CCXT: exchange.create_order with type="market", category="linear"
  - Funding rate fetched via exchange.fetch_funding_rate(symbol_perp)
  - Perp symbol format: "ETH/USDT:USDT" (CCXT unified format for linear perps)

Important Notes:
  - Leverage is set to 1x by default. This means NO margin amplification.
    You need the equivalent USDT as margin (same as holding spot).
  - In isolated margin mode, max loss on the perp = initial margin.
  - Funding rate: positive rate → shorts RECEIVE payment (bullish market).
                  negative rate → shorts PAY (bearish market).
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import config
from logger import get_logger, log_trade_event

log = get_logger(__name__)


# ─── Perp Symbol Mapping ──────────────────────────────────────────────────────
# CCXT unified format for Bybit USDT-M linear perpetuals
# Spot: "ETH/USDT"  →  Perp: "ETH/USDT:USDT"

def to_perp_symbol(spot_symbol: str) -> str:
    """Convert spot symbol to CCXT linear perp symbol notation."""
    base, quote = spot_symbol.split("/")
    return f"{base}/{quote}:{quote}"   # e.g. "ETH/USDT:USDT"


# ─── Hedge Position Tracking ──────────────────────────────────────────────────

@dataclass
class HedgePosition:
    symbol:           str
    perp_symbol:      str
    quantity:         float
    entry_price:      float
    entry_time:       float           = field(default_factory=time.time)
    last_funding_ts:  float           = field(default_factory=time.time)
    accrued_funding:  float           = 0.0   # Positive = received, Negative = paid
    bybit_order_id:   Optional[str]   = None

    @property
    def hours_held(self) -> float:
        return (time.time() - self.entry_time) / 3600.0

    @property
    def position_value_usdt(self) -> float:
        return self.entry_price * self.quantity


# ─── HedgeManager ─────────────────────────────────────────────────────────────

class HedgeManager:
    """
    Manages delta-neutral perpetual futures short positions on Bybit.
    One instance shared across the engine. Thread-safe for asyncio use.
    """

    def __init__(self, bybit_exchange):
        """
        Args:
            bybit_exchange: Authenticated Bybit CCXT Pro exchange instance.
                            Must have Derivatives trading permission enabled.
        """
        self._bybit = bybit_exchange
        self._positions: Dict[str, HedgePosition] = {}      # symbol → position
        self._funding_cache: Dict[str, Tuple[float, float]] = {}  # symbol → (rate, fetch_ts)
        self._enabled = config.ENABLE_DELTA_HEDGE

        if not self._enabled:
            log.info("[Hedge] Delta-neutral hedging is DISABLED via config")
        else:
            log.info(
                f"[Hedge] Delta-neutral hedge manager initialized | "
                f"Leverage: {config.HEDGE_LEVERAGE}x | "
                f"Mode: {'LIVE' if config.EXECUTION_MODE == 'LIVE' else 'PAPER'}"
            )

    # ─── Public API ───────────────────────────────────────────────────────────

    async def open_hedge(self, symbol: str, quantity: float, spot_price: float) -> bool:
        """
        Open a SHORT perpetual position on Bybit to hedge spot inventory.

        Call this immediately after pre-staging crypto on Bybit spot.

        Args:
            symbol:      Spot symbol e.g. "ETH/USDT"
            quantity:    Amount of base asset to hedge (same as spot qty)
            spot_price:  Current spot price (used to set leverage margin)

        Returns:
            True if hedge opened successfully, False otherwise.
        """
        if not self._enabled:
            return True   # No-op, not hedging

        if symbol in self._positions:
            log.warning(f"[Hedge] Already have an open hedge for {symbol} — skipping duplicate")
            return True

        perp_symbol = to_perp_symbol(symbol)

        # Check funding rate is within acceptable threshold before opening
        funding_rate = await self.get_funding_rate(symbol)
        if funding_rate is not None and funding_rate < config.MAX_FUNDING_RATE_THRESHOLD:
            # Rate is very negative: shorts pay longs — hedging is expensive right now
            log.warning(
                f"[Hedge] {symbol} funding rate {funding_rate*100:.4f}% is highly negative "
                f"(shorts pay). Skipping hedge to avoid excessive cost."
            )
            return False

        log.info(
            f"[Hedge] Opening SHORT hedge: {quantity:.6f} {symbol.split('/')[0]} "
            f"perp ({perp_symbol}) | Entry ~${spot_price:.4f} | "
            f"Funding rate: {(funding_rate or 0)*100:.4f}%/8h"
        )

        if config.EXECUTION_MODE == "PAPER":
            return self._paper_open_hedge(symbol, perp_symbol, quantity, spot_price)

        # ── LIVE: Set leverage, then open short perp ──────────────────────────
        try:
            # 1. Set leverage (idempotent — safe to call multiple times)
            await self._bybit.set_leverage(config.HEDGE_LEVERAGE, perp_symbol, params={
                "category": "linear",
                "buyLeverage": str(int(config.HEDGE_LEVERAGE)),
                "sellLeverage": str(int(config.HEDGE_LEVERAGE)),
            })

            # 2. Place market SELL order on perp (= enter short)
            order = await self._bybit.create_order(
                symbol=perp_symbol,
                type="market",
                side="sell",       # SHORT = selling into perp
                amount=quantity,
                params={
                    "category":    "linear",
                    "positionIdx": 0,        # One-way mode
                },
            )

            entry_price = float(order.get("average") or spot_price)
            order_id    = order.get("id")

            self._positions[symbol] = HedgePosition(
                symbol=symbol,
                perp_symbol=perp_symbol,
                quantity=quantity,
                entry_price=entry_price,
                bybit_order_id=order_id,
            )

            log_trade_event(
                log, "info",
                f"[Hedge] ✅ SHORT hedge opened: {symbol}",
                symbol=symbol,
                perp_symbol=perp_symbol,
                quantity=quantity,
                entry_price=entry_price,
                order_id=order_id,
                funding_rate=funding_rate,
            )
            return True

        except Exception as e:
            log.error(f"[Hedge] Failed to open hedge for {symbol}: {e}")
            return False

    async def close_hedge(self, symbol: str, quantity: Optional[float] = None) -> Tuple[bool, float]:
        """
        Close the SHORT perp hedge. Call this simultaneously with the spot SELL leg.

        Args:
            symbol:   Same symbol as the open hedge
            quantity: Amount to close. Defaults to full position size.

        Returns:
            (success: bool, realized_funding_pnl: float)
            funding_pnl is positive if funding was received, negative if paid.
        """
        if not self._enabled:
            return True, 0.0

        position = self._positions.get(symbol)
        if not position:
            log.warning(f"[Hedge] No open hedge found for {symbol} — nothing to close")
            return True, 0.0

        close_qty   = quantity or position.quantity
        perp_symbol = position.perp_symbol
        funding_pnl = position.accrued_funding

        log.info(
            f"[Hedge] Closing SHORT hedge: {close_qty:.6f} {symbol.split('/')[0]} "
            f"perp | Held {position.hours_held:.1f}h | "
            f"Accrued funding: ${funding_pnl:+.4f}"
        )

        if config.EXECUTION_MODE == "PAPER":
            return self._paper_close_hedge(symbol, funding_pnl)

        # ── LIVE: Buy back perp to close short ───────────────────────────────
        try:
            order = await self._bybit.create_order(
                symbol=perp_symbol,
                type="market",
                side="buy",        # BUY to close SHORT
                amount=close_qty,
                params={
                    "category":    "linear",
                    "positionIdx": 0,
                    "reduceOnly":  True,   # Critical: only reduce, never flip
                },
            )

            close_price = float(order.get("average", 0))
            price_pnl   = (position.entry_price - close_price) * close_qty  # Short P&L from price move
            total_pnl   = price_pnl + funding_pnl

            del self._positions[symbol]

            log_trade_event(
                log, "info",
                f"[Hedge] ✅ SHORT hedge closed: {symbol}",
                symbol=symbol,
                quantity=close_qty,
                entry_price=position.entry_price,
                close_price=close_price,
                price_pnl=price_pnl,
                funding_pnl=funding_pnl,
                total_hedge_pnl=total_pnl,
            )
            return True, funding_pnl

        except Exception as e:
            log.error(f"[Hedge] Failed to close hedge for {symbol}: {e}")
            return False, 0.0

    # ─── Funding Rate ─────────────────────────────────────────────────────────

    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        """
        Fetch the current funding rate for a symbol's USDT-M perp.
        Caches result for 60 seconds to avoid excessive REST calls.

        Returns:
            funding_rate per 8-hour period (e.g. 0.0001 = 0.01%).
            Positive = longs pay shorts (shorts receive).
            Negative = shorts pay longs (shorts pay).
        """
        now = time.time()
        cached_rate, cached_ts = self._funding_cache.get(symbol, (None, 0))

        if cached_rate is not None and (now - cached_ts) < 60:
            return cached_rate

        perp_symbol = to_perp_symbol(symbol)

        try:
            fr_data = await self._bybit.fetch_funding_rate(perp_symbol, params={"category": "linear"})
            rate    = float(fr_data.get("fundingRate", 0.0))
            self._funding_cache[symbol] = (rate, now)
            log.debug(f"[Hedge] Funding rate {symbol}: {rate*100:.4f}%/8h")
            return rate

        except Exception as e:
            log.warning(f"[Hedge] Could not fetch funding rate for {symbol}: {e}")
            return None

    def estimate_funding_cost(self, symbol: str, quantity: float, price: float) -> float:
        """
        Estimate the funding cost for a given symbol and position size
        for ONE 8-hour period. Use this in quant_node profit calc.

        Returns:
            Cost in USDT (negative = you pay, positive = you receive).
        """
        cached_rate, cached_ts = self._funding_cache.get(symbol, (None, 0))
        rate = cached_rate if cached_rate is not None else 0.0001  # Default 0.01%/8h

        # funding_pnl = rate * notional_value
        # Negative rate × short position = short PAYS (we want this as a cost)
        return rate * (quantity * price)   # Positive means we receive funding

    def update_accrued_funding(self, symbol: str, funding_payment: float) -> None:
        """
        Update the accrued funding P&L for an open position.
        Called periodically (every 8h) by the funding monitor.
        """
        if symbol in self._positions:
            self._positions[symbol].accrued_funding += funding_payment
            log.info(
                f"[Hedge] Funding payment for {symbol}: ${funding_payment:+.4f} | "
                f"Total accrued: ${self._positions[symbol].accrued_funding:+.4f}"
            )

    # ─── Paper Helpers ────────────────────────────────────────────────────────

    def _paper_open_hedge(
        self, symbol: str, perp_symbol: str, quantity: float, price: float
    ) -> bool:
        self._positions[symbol] = HedgePosition(
            symbol=symbol,
            perp_symbol=perp_symbol,
            quantity=quantity,
            entry_price=price,
            bybit_order_id="PAPER",
        )
        log.info(
            f"📋 [PAPER Hedge] SHORT {quantity:.6f} {symbol.split('/')[0]} "
            f"perp @ ~${price:.2f} opened (simulated)"
        )
        return True

    def _paper_close_hedge(self, symbol: str, funding_pnl: float) -> Tuple[bool, float]:
        if symbol in self._positions:
            pos = self._positions.pop(symbol)
            log.info(
                f"📋 [PAPER Hedge] SHORT {pos.quantity:.6f} "
                f"{symbol.split('/')[0]} perp closed (simulated) | "
                f"Simulated funding: ${funding_pnl:+.4f}"
            )
        return True, funding_pnl

    # ─── Introspection ─────────────────────────────────────────────────────────

    def get_open_positions(self) -> Dict[str, dict]:
        return {
            sym: {
                "perp_symbol":     pos.perp_symbol,
                "quantity":        pos.quantity,
                "entry_price":     pos.entry_price,
                "hours_held":      round(pos.hours_held, 2),
                "position_value":  round(pos.position_value_usdt, 2),
                "accrued_funding": round(pos.accrued_funding, 6),
            }
            for sym, pos in self._positions.items()
        }

    def has_hedge(self, symbol: str) -> bool:
        return symbol in self._positions
