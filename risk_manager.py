"""
risk_manager.py — Real-time risk control layer.

Enforces trading guardrails before any order reaches the exchange:
  - Daily P&L cap (circuit breaker): stop trading if daily losses exceed limit
  - Position tracking: prevent overlapping trades on the same symbol
  - Dynamic position sizing: calculate order quantity from USDT budget
  - Max concurrent positions: global cap across all symbols

Thread-safe for asyncio use (single-threaded event loop, no locks needed).
"""

import calendar
import time
from typing import Dict, Optional, Tuple

import config
from logger import get_logger, log_trade_event

log = get_logger(__name__)


class RiskManager:
    """
    Stateful risk controller. One instance shared across the engine.
    All methods are synchronous (safe to call in async context without await).
    """

    def __init__(self) -> None:
        self.daily_pnl: float            = 0.0       # Running P&L for today (USDT)
        self.daily_loss_breached: bool   = False      # Circuit breaker flag
        self._day_start_ts: float        = self._current_day_start()
        self._open_positions: Dict[str, int] = {}    # symbol → open count
        # Per-symbol consecutive failure counter (resets on success).
        # Triggers _SYMBOL_MAX_FAILURES → symbol halted for the session.
        self._symbol_failures: Dict[str, int] = {}

    _SYMBOL_MAX_FAILURES: int = 3  # Halt a symbol after N consecutive exec failures

    # ─── Internal Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _current_day_start() -> float:
        """
        Return UTC midnight timestamp of the current day.

        FIX: previously used time.mktime(time.gmtime()) which interprets the
        UTC struct_time as LOCAL time — causing rollover at 5:30 AM IST instead
        of UTC midnight. calendar.timegm() correctly treats struct_time as UTC.
        """
        t = time.gmtime()
        return float(calendar.timegm((t.tm_year, t.tm_mon, t.tm_mday, 0, 0, 0, 0, 1, 0)))

    def _check_day_rollover(self) -> None:
        """Reset daily P&L and circuit breaker at UTC midnight."""
        now_day_start = self._current_day_start()
        if now_day_start > self._day_start_ts:
            if self.daily_pnl != 0.0:
                log.info(
                    f"[Risk] Day rolled over. Final daily P&L was ${self.daily_pnl:.4f}. Resetting."
                )
            self.daily_pnl          = 0.0
            self.daily_loss_breached = False
            self._open_positions    = {}
            self._symbol_failures   = {}   # Reset per-symbol halt on new day
            self._day_start_ts      = now_day_start

    # ─── Trade Gate ───────────────────────────────────────────────────────────

    def can_trade(self, symbol: str, estimated_net_profit: float) -> Tuple[bool, str]:
        """
        Check all risk conditions before allowing a trade.

        Returns:
            (allowed: bool, reason: str)
        """
        self._check_day_rollover()

        # 1. Daily loss circuit breaker
        if self.daily_loss_breached:
            return False, f"Circuit breaker active: daily loss ${abs(self.daily_pnl):.4f} exceeded limit ${config.MAX_DAILY_LOSS_USDT:.2f}"

        # Preventively check if we're approaching the limit
        if self.daily_pnl <= -config.MAX_DAILY_LOSS_USDT:
            self.daily_loss_breached = True
            log_trade_event(
                log, "warning",
                f"🛑 Daily loss limit hit — circuit breaker activated",
                daily_pnl=self.daily_pnl,
                limit=config.MAX_DAILY_LOSS_USDT,
            )
            return False, f"Daily loss limit reached (${self.daily_pnl:.4f})"

        # 2. Min profit threshold (config-driven, already calculated by quant)
        if estimated_net_profit < config.MIN_NET_PROFIT_USDT:
            return False, f"Profit ${estimated_net_profit:.6f} below minimum ${config.MIN_NET_PROFIT_USDT:.4f}"

        # 3. Global concurrent position cap
        total_open = sum(self._open_positions.values())
        if total_open >= config.MAX_CONCURRENT_POSITIONS:
            return False, f"Max concurrent positions reached ({total_open}/{config.MAX_CONCURRENT_POSITIONS})"

        # 4. Per-symbol duplicate check (no overlapping trades on same pair)
        if self._open_positions.get(symbol, 0) > 0:
            return False, f"Position already open for {symbol}"

        # 5. Per-symbol consecutive failure circuit breaker
        failures = self._symbol_failures.get(symbol, 0)
        if failures >= self._SYMBOL_MAX_FAILURES:
            return False, (
                f"{symbol} halted after {failures} consecutive execution failures. "
                f"Investigate manually and restart the engine to reset."
            )

        return True, "OK"

    # ─── Position Lifecycle ───────────────────────────────────────────────────

    def open_position(self, symbol: str) -> None:
        """Register an open position when an order is submitted."""
        self._open_positions[symbol] = self._open_positions.get(symbol, 0) + 1
        log.debug(f"[Risk] Position opened: {symbol} | Open: {dict(self._open_positions)}")

    def close_position(self, symbol: str, realized_pnl: float) -> None:
        """
        Register a closed position and update daily P&L.
        Call this after BOTH legs are confirmed filled (or abandoned).
        """
        count = self._open_positions.get(symbol, 0)
        if count > 0:
            self._open_positions[symbol] = count - 1
        if self._open_positions.get(symbol, 0) == 0:
            self._open_positions.pop(symbol, None)

        self.daily_pnl += realized_pnl

        # Reset per-symbol failure counter on a successful close
        if realized_pnl > 0:
            self._symbol_failures.pop(symbol, None)

        log_trade_event(
            log, "info",
            f"[Risk] Trade closed: {symbol}",
            symbol=symbol,
            realized_pnl=realized_pnl,
            daily_pnl=self.daily_pnl,
            open_positions=dict(self._open_positions),
        )

        log.info(
            f"[Risk] Daily P&L: ${self.daily_pnl:+.4f} USDT | "
            f"Open positions: {dict(self._open_positions)}"
        )

    def record_symbol_failure(self, symbol: str) -> None:
        """Increment consecutive failure counter for a symbol."""
        self._symbol_failures[symbol] = self._symbol_failures.get(symbol, 0) + 1
        count = self._symbol_failures[symbol]
        log.warning(
            f"[Risk] {symbol} consecutive failures: {count}/{self._SYMBOL_MAX_FAILURES}"
            + (f" — HALTED for session!" if count >= self._SYMBOL_MAX_FAILURES else "")
        )

    def record_symbol_success(self, symbol: str) -> None:
        """Reset failure counter on a confirmed successful trade."""
        if symbol in self._symbol_failures:
            self._symbol_failures.pop(symbol)
            log.debug(f"[Risk] {symbol} failure counter cleared.")


    # ─── Position Sizing ──────────────────────────────────────────────────────

    def calculate_order_qty(
        self,
        symbol: str,
        ask_price: float,
        exchange,
        usdt_budget: Optional[float] = None,
    ) -> float:
        """
        Calculate safe order quantity based on USDT budget and exchange precision rules.

        Args:
            symbol:      Trading pair e.g. "ETH/USDT"
            ask_price:   Current ask price on the buy exchange
            exchange:    CCXT exchange instance (for precision/min order info)
            usdt_budget: USDT to spend (defaults to config.MAX_ORDER_USDT)

        Returns:
            Rounded-down quantity respecting exchange precision requirements.
        """
        budget = usdt_budget or config.MAX_ORDER_USDT
        raw_qty = budget / ask_price

        # Apply exchange's precision rounding
        try:
            market = exchange.market(symbol)
            precision = market.get("precision", {}).get("amount", 6)
            min_amount = market.get("limits", {}).get("amount", {}).get("min", 0)

            # Round down to precision
            qty = exchange.amount_to_precision(symbol, raw_qty)
            qty = float(qty)

            if qty < min_amount:
                log.warning(
                    f"[Risk] Calculated qty {qty} < min order size {min_amount} for {symbol}. "
                    f"Skipping — increase MAX_ORDER_USDT or choose a lower-priced pair."
                )
                return 0.0

        except Exception as e:
            log.warning(f"[Risk] Precision lookup failed for {symbol}: {e} — using raw qty")
            qty = round(raw_qty, 6)

        return qty

    # ─── Status ───────────────────────────────────────────────────────────────

    def status_summary(self) -> dict:
        return {
            "daily_pnl":           round(self.daily_pnl, 6),
            "daily_loss_breached": self.daily_loss_breached,
            "open_positions":      dict(self._open_positions),
            "total_open":          sum(self._open_positions.values()),
        }
