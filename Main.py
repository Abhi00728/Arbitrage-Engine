"""
Main.py — Production Arbitrage Engine Orchestrator

Entry point for the QuantSwarm Arbitrage Engine.
Wires together all modules and manages the full lifecycle:
  1. Config validation + logging setup
  2. Exchange client initialization + market loading
  3. Balance monitor (pre-staged fund checker)
  4. Market feed WebSocket streams
  5. Parallel execution workers (quant DAG consumers)
  6. Graceful shutdown on SIGINT/SIGTERM

Architecture:
  - MarketFeedManager produces tick signals → MARKET_EVENT_QUEUE
  - N execution_worker coroutines consume from the queue
  - Each worker runs the LangGraph DAG (scout→quant→risk→execute)
  - BalanceMonitor runs independently, polling balances every 30s
  - RiskManager and OrderExecutor are singletons shared across workers

The cross-exchange latency problem is solved architecturally:
  Pre-stage balances on BOTH exchanges. The balance_monitor.check_can_execute()
  gate ensures trades only fire when both sides have sufficient funds.
  No runtime transfers occur — you manually rebalance when alerted.
"""

import asyncio
import os
import signal
import sys
import time
from typing import List

# ── Core modules ─────────────────────────────────────────────────────────────
import config
from logger import setup_logging, get_logger, log_trade_event
from exchange_clients import create_exchange, close_all, load_markets_all
from market_feed import MarketFeedManager, MARKET_EVENT_QUEUE, mark_symbol_dequeued
from balance_monitor import BalanceMonitor
from risk_manager import RiskManager
from order_executor import OrderExecutor
from hedge_manager import HedgeManager
from rebalancer import Rebalancer
from notifier import Notifier
from quant_engine import build_graph, SwarmState

# ── DB ────────────────────────────────────────────────────────────────────────
try:
    from mysql.connector import pooling as mysql_pooling
    _DB_AVAILABLE = True
except ImportError:
    _DB_AVAILABLE = False

# ─── Logger (set up before anything else) ────────────────────────────────────
setup_logging(log_dir=config.LOG_DIR, log_level=config.LOG_LEVEL)
log = get_logger(__name__)

# ─── Globals (populated in main()) ────────────────────────────────────────────
_running   = True
_stop_event: asyncio.Event | None = None  # Set by signal handler for graceful drain


# ─── DB Pool Factory ──────────────────────────────────────────────────────────

def _init_db_pool():
    """Attempt to initialize MySQL connection pool. Returns None on failure."""
    if not _DB_AVAILABLE:
        log.warning("[DB] mysql-connector-python not installed — trades will not be logged to DB")
        return None
    try:
        pool = mysql_pooling.MySQLConnectionPool(**config.DB_CONFIG)
        log.info("[DB] MySQL connection pool initialized")
        return pool
    except Exception as e:
        log.warning(f"[DB] Could not connect to MySQL: {e} — DB logging disabled")
        return None


# ─── Execution Worker ─────────────────────────────────────────────────────────

async def execution_worker(
    worker_id: int,
    graph,
    stop_event: asyncio.Event,
) -> None:
    """
    Consumes symbol tick signals from MARKET_EVENT_QUEUE and runs the quant DAG.
    Multiple workers can run in parallel; each takes one signal at a time.
    """
    log.info(f"[Worker-{worker_id}] Ready — consuming from market event queue")

    while not stop_event.is_set():
        try:
            # Wait for a tick signal with a timeout to allow clean shutdown
            symbol = await asyncio.wait_for(MARKET_EVENT_QUEUE.get(), timeout=1.0)

        except asyncio.TimeoutError:
            continue

        except asyncio.CancelledError:
            break

        try:
            initial_state: SwarmState = {
                "symbol":          symbol,
                "market_data":     {},
                "decision":        "PASS",
                "net_profit":      0.0,
                "order_quantity":  0.0,
                "risk_blocked":    False,
                "risk_reason":     "",
                "execution_status": "",
                "realized_pnl":    0.0,
            }

            final_state = await graph.ainvoke(initial_state)

            if final_state.get("decision") == "FILLED":
                log.info(
                    f"[Worker-{worker_id}] ✅ {symbol} filled | "
                    f"Realized: ${final_state.get('realized_pnl', 0.0):.4f}"
                )

        except asyncio.CancelledError:
            break

        except Exception as e:
            log.error(f"[Worker-{worker_id}] Exception processing {symbol}: {e}", exc_info=True)

        finally:
            # Remove the symbol from the dedup set so future ticks can be queued
            mark_symbol_dequeued(symbol)
            MARKET_EVENT_QUEUE.task_done()

    log.info(f"[Worker-{worker_id}] Stopped")


# ─── Status Printer ───────────────────────────────────────────────────────────

async def status_printer(risk_manager: RiskManager, stop_event: asyncio.Event) -> None:
    """Prints a periodic engine status summary every 60 seconds."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=60.0)
        except asyncio.TimeoutError:
            pass

        if not stop_event.is_set():
            status = risk_manager.status_summary()
            log.info(
                f"[Engine Status] Daily P&L: ${status['daily_pnl']:+.4f} | "
                f"Open positions: {status['total_open']} | "
                f"Circuit breaker: {'🔴 ACTIVE' if status['daily_loss_breached'] else '🟢 OK'} | "
                f"Queue depth: {MARKET_EVENT_QUEUE.qsize()}"
            )


async def _hourly_summary_loop(
    notifier, risk_manager: RiskManager, stop_event: asyncio.Event
) -> None:
    """Send an hourly P&L summary to Telegram every 60 minutes."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=3600.0)  # 1 hour
        except asyncio.TimeoutError:
            pass
        if not stop_event.is_set():
            await notifier.send_hourly_summary(risk_manager)


# ─── Main ────────────────────────────────────────────────────────────────────

async def main() -> None:
    global _running

    # ── 1. Print config summary ────────────────────────────────────────────────
    config.print_config_summary()

    if config.EXECUTION_MODE == "LIVE":
        log.warning("🔴 LIVE MODE ACTIVE — Real orders will be placed on exchanges!")
        await asyncio.sleep(2)  # Brief pause to allow the operator to abort if needed

    # ── 2. Initialize database pool ────────────────────────────────────────────
    db_pool = _init_db_pool()

    # ── 3. Create exchange clients ─────────────────────────────────────────────
    log.info("[Engine] Initializing exchange clients...")
    binance = create_exchange("binance")
    bybit   = create_exchange("bybit")

    # ── 4. Load market metadata ────────────────────────────────────────────────
    log.info("[Engine] Loading market metadata (precision rules, min sizes)...")
    await load_markets_all()

    # ── 5. Initialize core services ──────────────────────────────────
    risk_manager   = RiskManager()
    order_executor = OrderExecutor(binance, bybit, db_pool=db_pool)
    hedge_manager  = HedgeManager(bybit)
    rebalancer     = Rebalancer(bybit, hedge_manager)
    notifier       = Notifier(risk_manager=risk_manager, rebalancer=rebalancer)

    # ── 6. Build the LangGraph DAG ─────────────────────────────────
    log.info("[Engine] Compiling LangGraph execution DAG...")
    graph = build_graph(
        risk_manager, order_executor, hedge_manager,
        rebalancer, notifier,
        binance, bybit,
    )
    log.info("[Engine] DAG compiled ✅")

    # ── 7. Start balance monitor ──────────────────────────────────
    balance_monitor = BalanceMonitor(binance, bybit)
    balance_task    = None

    if config.EXECUTION_MODE == "LIVE":
        # In LIVE mode: do an immediate balance fetch before starting
        log.info("[Engine] Fetching initial balance snapshot...")
        await balance_monitor.fetch_once()
        balance_task = balance_monitor.start()
    else:
        # In PAPER mode: inject synthetic pre-staged balances so the balance gate
        # doesn't block every trade. These simulate what you'd have after manually
        # funding both exchange sides before going live.
        log.info("[Engine] PAPER mode — injecting synthetic balances for gate simulation")
        from balance_monitor import LIVE_BALANCES, _last_fetch_ts
        import time as _time
        LIVE_BALANCES["binance"] = {"USDT": 500.0}
        LIVE_BALANCES["bybit"]   = {
            "USDT": 500.0,
            "ETH":  0.5,
            "SOL":  5.0,
            "BNB":  1.0,
            "POL":  200.0,
        }
        _last_fetch_ts["binance"] = _time.time()
        _last_fetch_ts["bybit"]   = _time.time()


    # ── 8. Start market feed streams ──────────────────────────────────────────────
    global _stop_event
    stop_event   = asyncio.Event()
    _stop_event  = stop_event   # Wire to global so _handle_signal() can reach it
    feed_manager = MarketFeedManager(binance, bybit)
    feed_tasks   = feed_manager.start()

    # ── 9. Start execution workers (1 per SCAN_PAIR for parallelism) ───────────────
    n_workers    = max(2, len(config.SCAN_PAIRS))
    worker_tasks = [
        asyncio.create_task(
            execution_worker(i, graph, stop_event),
            name=f"exec-worker-{i}",
        )
        for i in range(n_workers)
    ]

    # ── 10. Status printer (console) ────────────────────────────────────
    status_task = asyncio.create_task(
        status_printer(risk_manager, stop_event),
        name="status-printer",
    )

    # ── 11. Telegram polling + hourly summary ────────────────────────────
    telegram_task = asyncio.create_task(
        notifier.start_polling(stop_event),
        name="telegram-polling",
    )
    hourly_task = asyncio.create_task(
        _hourly_summary_loop(notifier, risk_manager, stop_event),
        name="hourly-summary",
    )

    # ── 12. Funding monitor (tracks accrued 8-hourly funding on open hedges) ──────
    funding_monitor_task = asyncio.create_task(
        _funding_monitor_loop(hedge_manager, stop_event),
        name="funding-monitor",
    )

    # ── 13. Feed watchdog (alerts if a WebSocket worker dies silently) ────────────
    watchdog_task = asyncio.create_task(
        _feed_watchdog(feed_tasks, notifier, stop_event),
        name="feed-watchdog",
    )

    log.info(
        f"🚀 [Engine] QuantSwarm LIVE — {n_workers} workers | "
        f"{len(config.SCAN_PAIRS)} pairs | Mode: {config.EXECUTION_MODE}"
    )

    all_tasks = feed_tasks + worker_tasks + [
        status_task, telegram_task, hourly_task,
        funding_monitor_task, watchdog_task,
    ]
    if balance_task:
        all_tasks.append(balance_task)

    try:
        # Wait for stop_event (set by SIGTERM/SIGINT) rather than gathering all tasks.
        # This avoids a CancelledError cascade that kills in-flight DB writes.
        await stop_event.wait()

    except (KeyboardInterrupt, asyncio.CancelledError):
        pass

    # ── 12. Graceful shutdown ───────────────────────────────────────────────────
    finally:
        log.info("\n🛑 [Engine] Shutdown signal received — graceful teardown...")
        stop_event.set()

        # Stop feed streams
        await feed_manager.stop()

        # Stop balance monitor
        await balance_monitor.stop()

        # Cancel remaining tasks
        remaining = worker_tasks + [status_task]
        for task in remaining:
            task.cancel()
        await asyncio.gather(*remaining, return_exceptions=True)

        # Close exchange connections
        await close_all()

        # Final P&L report
        status = risk_manager.status_summary()
        log.info(
            f"📊 [Engine] Final session P&L: ${status['daily_pnl']:+.4f} USDT | "
            f"Circuit breaker was: {'ACTIVE' if status['daily_loss_breached'] else 'OK'}"
        )
        log.info("✅ [Engine] QuantSwarm shut down cleanly.")


# ── Funding Monitor ─────────────────────────────────────────────────────────────────

async def _funding_monitor_loop(
    hedge_manager,
    stop_event: asyncio.Event,
    interval_s: int = 8 * 3600,  # every 8 hours (Binance/Bybit funding cycle)
) -> None:
    """
    Periodically update accrued funding costs on all open hedge positions.

    Without this loop, HedgePosition.accrued_funding stays at 0.0 forever,
    meaning the quant engine always sees $0 carry cost on long-running hedges.
    This causes real money losses on positions held across multiple funding periods.
    """
    while not stop_event.is_set():
        try:
            # Wait for the next funding period (or shutdown)
            await asyncio.wait_for(stop_event.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass  # Timer expired normally — update funding now
        if stop_event.is_set():
            break

        open_positions = hedge_manager.get_open_positions()
        for symbol in list(open_positions.keys()):
            try:
                rate = await hedge_manager.get_funding_rate(symbol)
                if rate is not None:
                    position_value = open_positions[symbol].get("position_value_usdt", 0.0)
                    payment = rate * position_value
                    hedge_manager.update_accrued_funding(symbol, payment)
                    log.info(
                        f"[FundingMonitor] {symbol}: rate={rate*100:.4f}% | "
                        f"payment=${payment:.4f} | value=${position_value:.2f}"
                    )
            except Exception as e:
                log.warning(f"[FundingMonitor] Failed to update funding for {symbol}: {e}")


# ── Feed Watchdog ──────────────────────────────────────────────────────────────────

async def _feed_watchdog(
    feed_tasks: list,
    notifier,
    stop_event: asyncio.Event,
    check_interval_s: int = 30,
) -> None:
    """
    Periodically checks that all WebSocket feed tasks are still alive.

    If a feed task has died silently (exception caught internally but loop exited),
    the market matrix stops updating. The staleness filter will eventually block
    new trades, but without an alert you'd have no idea why the engine went quiet.
    """
    while not stop_event.is_set():
        await asyncio.sleep(check_interval_s)
        dead = [t for t in feed_tasks if t.done() and not stop_event.is_set()]
        for task in dead:
            exc = task.exception() if not task.cancelled() else "CancelledError"
            msg = (
                f"🚨 Feed task `{task.get_name()}` has DIED — {exc}. "
                f"Market data may be stale. Engine idling until reconnect."
            )
            log.critical(msg)
            await notifier.alert_critical(msg)


# ── Signal handling for production deployment ───────────────────────────────────

def _handle_signal(sig, frame):
    """
    SIGINT / SIGTERM handler.

    FIX: previously called task.cancel() on ALL tasks which immediately killed
    in-flight DB writes and mid-execution unwind tasks, corrupting the audit trail.

    Now just sets the stop_event. main() watches it and triggers graceful teardown
    (waits for in-flight tasks to complete before exiting).
    """
    log.warning(f"[Engine] Signal {sig} received — requesting graceful shutdown...")
    if _stop_event is not None:
        # Schedule set() on the running event loop — signal handlers run in the
        # main thread, call_soon_threadsafe is required to safely cross into asyncio.
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(_stop_event.set)
    else:
        # Fallback if called before main() has set up the event
        sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log.critical(f"[Engine] Fatal unhandled exception: {e}", exc_info=True)
        sys.exit(1)
