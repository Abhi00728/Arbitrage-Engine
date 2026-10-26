"""
order_executor.py — Simultaneous 3-leg arbitrage execution engine.

Fires all three execution legs at exactly the same time:
  Leg 1: BUY  base asset on Binance SPOT           (asyncio.gather)
  Leg 2: SELL base asset on Bybit   SPOT           (asyncio.gather)
  Leg 3: CLOSE SHORT on Bybit USDT-M PERP (hedge)  (asyncio.gather)

All 3 submit in parallel — maximum inter-leg latency is the slowest single
exchange RTT, NOT the sum. This is the minimum achievable execution window.

Handles:
  - Partial fills with imbalance detection + automatic emergency unwind
  - Hedge close failure (logs CRITICAL but does not fail the arb result)
  - PAPER mode simulation with accurate timing
  - DB audit trail for every trade
  - CCXT exception triage (auth errors never retried, rate-limit gets longer backoff)
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

try:
    from ccxt import AuthenticationError, InsufficientFunds, RateLimitExceeded, NetworkError
except ImportError:  # ccxt not installed yet (e.g. latency_test mock environment)
    AuthenticationError = InsufficientFunds = RateLimitExceeded = NetworkError = Exception

import config
from logger import get_logger, log_trade_event

log = get_logger(__name__)


# ─── Trade Result ─────────────────────────────────────────────────────────────

@dataclass
class TradeResult:
    symbol:         str
    status:         str         # "FILLED" | "PARTIAL" | "FAILED" | "PAPER"
    buy_exchange:   str
    sell_exchange:  str
    requested_qty:  float
    buy_filled_qty: float       = 0.0
    sell_filled_qty: float      = 0.0
    buy_avg_price:  float       = 0.0
    sell_avg_price: float       = 0.0
    estimated_pnl:  float       = 0.0
    realized_pnl:   float       = 0.0
    execution_ms:   int         = 0     # Wall-clock time for both legs
    error:          Optional[str] = None
    raw:            Dict[str, Any] = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        return self.status in ("FILLED", "PAPER")

    @property
    def has_imbalance(self) -> bool:
        return abs(self.buy_filled_qty - self.sell_filled_qty) > 1e-8


# ─── Order Executor ───────────────────────────────────────────────────────────

class OrderExecutor:
    """
    Manages the actual placement of arbitrage orders across two exchanges.
    One instance is shared engine-wide.
    """

    def __init__(self, binance_exchange, bybit_exchange, db_pool=None):
        self._exchanges = {
            "binance": binance_exchange,
            "bybit":   bybit_exchange,
        }
        self._db_pool = db_pool

    # ─── Main Entry Point ─────────────────────────────────────────────────────

    async def execute_arb(
        self,
        symbol:        str,
        buy_exchange:  str,
        sell_exchange: str,
        quantity:      float,
        estimated_pnl: float,
        hedge_manager=None,     # Optional HedgeManager — closes perp short in parallel
    ) -> TradeResult:
        """
        Execute a complete arbitrage trade: simultaneous BUY + SELL + hedge close.

        If hedge_manager is provided and has an open position for this symbol,
        the SHORT perp close is fired simultaneously with the spot legs.

        Args:
            symbol:        e.g. "ETH/USDT"
            buy_exchange:  exchange name where we BUY  (e.g. "binance")
            sell_exchange: exchange name where we SELL (e.g. "bybit")
            quantity:      asset quantity for each leg
            estimated_pnl: expected profit in USDT (for logging comparison)
            hedge_manager: optional HedgeManager instance

        Returns:
            TradeResult with actual fill data.
        """
        if quantity <= 0:
            return TradeResult(
                symbol=symbol, status="FAILED",
                buy_exchange=buy_exchange, sell_exchange=sell_exchange,
                requested_qty=quantity,
                error="Invalid quantity (0 or negative)",
            )

        if config.EXECUTION_MODE == "PAPER":
            return await self._paper_execute(symbol, buy_exchange, sell_exchange, quantity, estimated_pnl, hedge_manager)
        else:
            return await self._live_execute(symbol, buy_exchange, sell_exchange, quantity, estimated_pnl, hedge_manager)

    # ─── Paper Execution ──────────────────────────────────────────────────────

    async def _paper_execute(
        self, symbol, buy_exchange, sell_exchange, quantity, estimated_pnl,
        hedge_manager=None,
    ) -> TradeResult:
        """Simulate all 3 execution legs — no real orders placed."""
        t0 = time.monotonic()
        await asyncio.sleep(0.005)   # Simulate ~5ms network round-trip
        elapsed_ms = int((time.monotonic() - t0) * 1000)

        # Simulate hedge close if manager has an open position
        funding_collected = 0.0
        if hedge_manager and hedge_manager.has_hedge(symbol):
            _, funding_collected = await hedge_manager.close_hedge(symbol, quantity)
            log.info(f"📋 [PAPER Hedge] Closed SHORT perp for {symbol} simultaneously")

        result = TradeResult(
            symbol=symbol,
            status="PAPER",
            buy_exchange=buy_exchange,
            sell_exchange=sell_exchange,
            requested_qty=quantity,
            buy_filled_qty=quantity,
            sell_filled_qty=quantity,
            estimated_pnl=estimated_pnl,
            realized_pnl=estimated_pnl + funding_collected,
            execution_ms=elapsed_ms,
        )

        log_trade_event(
            log, "info",
            f"📋 [PAPER] {symbol} | BUY {quantity} on {buy_exchange.upper()} | "
            f"SELL {quantity} on {sell_exchange.upper()} | "
            f"Est P&L: ${estimated_pnl:.4f} | Funding: ${funding_collected:+.5f}",
            symbol=symbol,
            mode="PAPER",
            quantity=quantity,
            estimated_pnl=estimated_pnl,
            funding_collected=funding_collected,
            execution_ms=elapsed_ms,
        )

        await self._log_to_db(result)
        return result

    # ─── Live Execution ───────────────────────────────────────────────────────

    async def _live_execute(
        self, symbol, buy_exchange, sell_exchange, quantity, estimated_pnl,
        hedge_manager=None,
    ) -> TradeResult:
        """
        Place BUY, SELL, and hedge-close orders simultaneously.

        asyncio.gather fires all 3 at the same instant:
          - Spot BUY  on Binance
          - Spot SELL on Bybit
          - Perp SHORT close on Bybit (if hedge is open)
        """
        buy_ex  = self._exchanges[buy_exchange]
        sell_ex = self._exchanges[sell_exchange]

        has_hedge = hedge_manager and hedge_manager.has_hedge(symbol) if hedge_manager else False
        n_legs    = 3 if has_hedge else 2

        log.info(
            f"⚡ [LIVE] Firing {n_legs}-leg execution: "
            f"BUY {quantity} {symbol} on {buy_exchange.upper()} | "
            f"SELL {quantity} {symbol} on {sell_exchange.upper()}"
            + (" | CLOSE SHORT PERP" if has_hedge else " | (no hedge to close)")
        )

        t0 = time.monotonic()

        # ── SIMULTANEOUS 3-LEG SUBMISSION (or 2-leg if no hedge) ──────────
        coroutines = [
            self._place_order_with_retry(buy_ex,  symbol, "buy",  quantity, buy_exchange),
            self._place_order_with_retry(sell_ex, symbol, "sell", quantity, sell_exchange),
        ]
        if has_hedge:
            coroutines.append(hedge_manager.close_hedge(symbol, quantity))

        results = await asyncio.gather(*coroutines, return_exceptions=True)

        elapsed_ms   = int((time.monotonic() - t0) * 1000)
        buy_result   = results[0]
        sell_result  = results[1]
        hedge_result = results[2] if has_hedge else (True, 0.0)

        # ── Unpack spot leg results ─────────────────────────────────────────
        buy_ok   = isinstance(buy_result,  dict)
        sell_ok  = isinstance(sell_result, dict)

        buy_filled  = float(buy_result.get("filled",   0)) if buy_ok  else 0.0
        sell_filled = float(sell_result.get("filled",  0)) if sell_ok else 0.0
        buy_price   = float(buy_result.get("average",  0)) if buy_ok  else 0.0
        sell_price  = float(sell_result.get("average", 0)) if sell_ok else 0.0

        # ── Unpack hedge result ──────────────────────────────────────────────
        funding_collected = 0.0
        if has_hedge:
            if isinstance(hedge_result, Exception):
                # Hedge close failed — CRITICAL. The perp short is still open.
                # The spot trade already completed. Log loudly for manual action.
                log.critical(
                    f"🔴 HEDGE CLOSE FAILED for {symbol} after spot trade! "
                    f"Short perp still open — MANUAL CLOSE REQUIRED on Bybit. Error: {hedge_result}"
                )
            elif isinstance(hedge_result, tuple) and len(hedge_result) == 2:
                hedge_ok, funding_collected = hedge_result
                if hedge_ok:
                    log.info(f"[Hedge] ✅ Perp close confirmed for {symbol} | Funding: ${funding_collected:+.5f}")
                else:
                    log.critical(f"🔴 HEDGE CLOSE RETURNED FAILURE for {symbol} — check Bybit positions manually!")

        # ── Calculate realized P&L from actual fill prices ───────────────────
        if buy_ok and sell_ok and buy_price > 0 and sell_price > 0:
            filled_qty   = min(buy_filled, sell_filled)
            gross_spread = (sell_price - buy_price) * filled_qty
            fee_cost     = (buy_price * filled_qty * config.FEE_TIERS.get(buy_exchange, 0.001)) + \
                           (sell_price * filled_qty * config.FEE_TIERS.get(sell_exchange, 0.001))
            realized_pnl = gross_spread - fee_cost + funding_collected
        else:
            realized_pnl = 0.0

        # ── Determine status ─────────────────────────────────────────────────
        if buy_ok and sell_ok:
            imbalance = abs(buy_filled - sell_filled)
            status = "FILLED" if imbalance < 1e-8 else "PARTIAL"
        else:
            status = "FAILED"
            errors = []
            if not buy_ok:  errors.append(f"BUY error: {buy_result}")
            if not sell_ok: errors.append(f"SELL error: {sell_result}")
            error_str = " | ".join(errors)
            log.error(f"🚨 [LIVE] Execution failure on {symbol}: {error_str}")

            # ── EMERGENCY UNWIND: auto-neutralise whichever leg filled ────────
            # One-sided fill = unhedged inventory exposure. Fire a compensating
            # market order immediately to flatten the position.
            if buy_ok and not sell_ok and buy_filled > 0:
                log.critical(
                    f"🔴 ONE-LEG FILL: BUY filled ({buy_filled:.6f}), SELL FAILED on {symbol}. "
                    f"Launching emergency SELL on {buy_exchange} to unwind inventory."
                )
                asyncio.create_task(
                    self._emergency_unwind(buy_exchange, symbol, buy_filled, "sell"),
                    name=f"emergency-unwind-buy-{symbol}",
                )
            elif sell_ok and not buy_ok and sell_filled > 0:
                log.critical(
                    f"🔴 ONE-LEG FILL: SELL filled ({sell_filled:.6f}), BUY FAILED on {symbol}. "
                    f"Launching emergency BUY on {sell_exchange} to unwind inventory."
                )
                asyncio.create_task(
                    self._emergency_unwind(sell_exchange, symbol, sell_filled, "buy"),
                    name=f"emergency-unwind-sell-{symbol}",
                )

        result = TradeResult(
            symbol=symbol,
            status=status,
            buy_exchange=buy_exchange,
            sell_exchange=sell_exchange,
            requested_qty=quantity,
            buy_filled_qty=buy_filled,
            sell_filled_qty=sell_filled,
            buy_avg_price=buy_price,
            sell_avg_price=sell_price,
            estimated_pnl=estimated_pnl,
            realized_pnl=realized_pnl,
            execution_ms=elapsed_ms,
            error=None if (buy_ok and sell_ok) else "One or more legs failed",
            raw={"buy": buy_result if buy_ok else {}, "sell": sell_result if sell_ok else {}},
        )

        if result.has_imbalance and status != "FAILED":
            log.warning(
                f"⚠️  [LIVE] Partial fill imbalance on {symbol}: "
                f"bought {buy_filled:.6f} / sold {sell_filled:.6f}. "
                f"Difference: {abs(buy_filled - sell_filled):.6f} — rebalance needed."
            )

        log_trade_event(
            log, "info" if result.is_success else "error",
            f"[LIVE] Trade result: {symbol} | status={status} | "
            f"realized_pnl=${realized_pnl:.4f} | funding=${funding_collected:+.5f} | latency={elapsed_ms}ms",
            symbol=symbol,
            status=status,
            buy_exchange=buy_exchange,
            sell_exchange=sell_exchange,
            buy_filled_qty=buy_filled,
            sell_filled_qty=sell_filled,
            buy_avg_price=buy_price,
            sell_avg_price=sell_price,
            estimated_pnl=estimated_pnl,
            realized_pnl=realized_pnl,
            funding_collected=funding_collected,
            execution_ms=elapsed_ms,
        )

        await self._log_to_db(result)
        return result

    # ─── Emergency Unwind ────────────────────────────────────────────────────

    async def _emergency_unwind(
        self,
        exchange_name: str,
        symbol: str,
        quantity: float,
        side: str,           # "sell" to unwind a stray BUY; "buy" to unwind a stray SELL
    ) -> None:
        """
        Fire a compensating market order when one arb leg fills and the other fails.

        This is the last line of defence against unhedged directional inventory.
        Runs as a background asyncio task — fires immediately after detection.
        Logs CRITICAL on failure so the operator knows manual action is required.
        """
        log.critical(
            f"🚨 [EMERGENCY UNWIND] {exchange_name.upper()} {side.upper()} "
            f"{quantity:.6f} {symbol} — neutralising failed leg..."
        )
        try:
            exchange = self._exchanges[exchange_name]
            order = await exchange.create_market_order(symbol, side, quantity)
            log.info(
                f"[Emergency] ✅ Unwind complete on {exchange_name.upper()}: "
                f"{side.upper()} {quantity:.6f} {symbol} | avg=${order.get('average'):.4f}"
            )
            log_trade_event(
                log, "warning",
                f"[Emergency] Unwind executed for {symbol} on {exchange_name}",
                symbol=symbol,
                exchange=exchange_name,
                side=side,
                quantity=quantity,
                avg_price=float(order.get("average", 0)),
            )
        except Exception as e:
            log.critical(
                f"🔴 [EMERGENCY UNWIND FAILED] {exchange_name.upper()} {side.upper()} "
                f"{quantity:.6f} {symbol}: {e}. "
                f"IMMEDIATE MANUAL CLOSE REQUIRED ON {exchange_name.upper()}."
            )

    # ─── Single Leg with Retry ────────────────────────────────────────────────

    async def _place_order_with_retry(
        self,
        exchange,
        symbol: str,
        side: str,
        quantity: float,
        exchange_name: str,
    ) -> dict:
        """
        Place a single market order with triage-aware retry logic.

        Fatal exceptions (AuthenticationError, InsufficientFunds) are never
        retried — they propagate immediately to trigger the imbalance handler.
        RateLimitExceeded uses a longer backoff.
        """
        for attempt in range(1, config.ORDER_RETRY_ATTEMPTS + 1):
            try:
                order = await exchange.create_market_order(symbol, side, quantity)
                log.info(
                    f"[Exec] {exchange_name.upper()} {side.upper()} order placed: "
                    f"id={order.get('id')} filled={order.get('filled')} avg={order.get('average')}"
                )
                return order

            except (AuthenticationError, InsufficientFunds) as e:
                # Fatal — never retry. Propagate immediately.
                log.critical(
                    f"[Exec] {exchange_name.upper()} {side.upper()} FATAL error "
                    f"({type(e).__name__}) — not retrying: {e}"
                )
                raise

            except RateLimitExceeded as e:
                log.warning(
                    f"[Exec] {exchange_name.upper()} rate limited on attempt {attempt}: {e}"
                )
                backoff = 1.0 * attempt  # Longer backoff for rate limits
                if attempt < config.ORDER_RETRY_ATTEMPTS:
                    await asyncio.sleep(backoff)
                else:
                    raise

            except Exception as e:
                log.error(
                    f"[Exec] {exchange_name.upper()} {side.upper()} attempt {attempt} failed: {e}"
                )
                if attempt < config.ORDER_RETRY_ATTEMPTS:
                    await asyncio.sleep(0.1 * attempt)
                else:
                    raise  # Propagate as exception to be caught by gather

    # ─── DB Logging ───────────────────────────────────────────────────────────

    async def _log_to_db(self, result: TradeResult) -> None:
        """Write trade outcome to the arbitrage_ledger table (non-blocking)."""
        if not self._db_pool:
            return
        try:
            # Use get_running_loop() — preferred over deprecated get_event_loop()
            await asyncio.get_running_loop().run_in_executor(None, self._db_write, result)
        except Exception as e:
            log.error(f"[DB] Failed to log trade to database: {e}")

    def _db_write(self, result: TradeResult) -> None:
        """Synchronous DB write (runs in thread pool)."""
        conn = None
        try:
            conn = self._db_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO arbitrage_ledger
                    (symbol, exchange_buy, exchange_sell,
                     buy_price, sell_price,
                     executed_qty, actual_buy_price, actual_sell_price,
                     net_profit_usdt, realized_pnl,
                     execution_ms, execution_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    result.symbol,
                    result.buy_exchange,
                    result.sell_exchange,
                    result.buy_avg_price,
                    result.sell_avg_price,
                    result.buy_filled_qty,
                    result.buy_avg_price,
                    result.sell_avg_price,
                    result.estimated_pnl,
                    result.realized_pnl,
                    result.execution_ms,
                    result.status,
                ),
            )
            conn.commit()
        except Exception as e:
            log.error(f"[DB] Write failed: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
