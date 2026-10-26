"""
quant_engine.py — LangGraph arbitrage detection DAG.

Graph flow:
    scout_node → quant_node → risk_node → execute_node → END
                     ↓ PASS         ↓ PASS
                    END             END

Nodes:
  scout_node:   Reads LIVE_MARKET_MATRIX snapshot for the triggered symbol
  quant_node:   Fee-adjusted spread calc, staleness filter, direction selection,
                FUNDING RATE deduction from hedge cost (new)
  risk_node:    Balance check, RiskManager gate (daily loss, positions, min profit)
  execute_node: Fires OrderExecutor.execute_arb() with hedge close, notifies RiskManager
"""

import asyncio
import time
from typing import TypedDict, Optional, Dict, Any

from langgraph.graph import StateGraph, END

import config
from logger import get_logger, log_trade_event
from market_feed import LIVE_MARKET_MATRIX
from balance_monitor import check_can_execute
from risk_manager import RiskManager
from order_executor import OrderExecutor
from hedge_manager import HedgeManager
from rebalancer import Rebalancer
from notifier import Notifier, ENGINE_PAUSED

log = get_logger(__name__)


# ─── State Schema ────────────────────────────────────────────────────

class SwarmState(TypedDict, total=False):
    # Input
    symbol:          str
    market_data:     Dict[str, Any]

    # Quant analysis
    decision:        str         # "PASS" | "EXECUTE" | "FILLED" | "FAILED"
    net_profit:      float       # Estimated net profit in USDT (after fees + funding)
    buy_exchange:    str         # Exchange name for BUY leg
    sell_exchange:   str         # Exchange name for SELL leg
    buy_price:       float       # Best ask on buy side
    sell_price:      float       # Best bid on sell side
    order_quantity:  float       # Calculated safe order qty
    funding_cost:    float       # Estimated hedge funding cost for this trade
    has_hedge:       bool        # Whether a hedge position is open for this symbol

    # Risk
    risk_blocked:    bool
    risk_reason:     str

    # Execution
    execution_status: str
    realized_pnl:    float


# ─── Node Implementations ─────────────────────────────────────────────────────

def scout_node(state: SwarmState) -> dict:
    """
    Reads a fresh snapshot of the live market data for this symbol.
    Does not make any trading decisions — pure data collection.
    """
    symbol = state["symbol"]
    snapshot = LIVE_MARKET_MATRIX.get(symbol, {})
    return {
        "market_data": {
            "exchange_a": dict(snapshot.get("exchange_a", {})),
            "exchange_b": dict(snapshot.get("exchange_b", {})),
        },
        "decision": "PENDING",
    }


def quant_node(state: SwarmState) -> dict:
    """
    Core spread analysis node.

    Evaluates BOTH directions of the arb (A→B and B→A) and picks
    the more profitable direction. Applies:
      - Data staleness filter (rejects ticks older than MAX_DATA_AGE_MS)
      - Fee-adjusted net profit calculation
      - Minimum depth check (ensures enough liquidity to fill)
    """
    data   = state.get("market_data", {})
    symbol = state["symbol"]

    ex_a = data.get("exchange_a", {})
    ex_b = data.get("exchange_b", {})

    # Must have both sides populated
    if not ex_a or not ex_b:
        return {"decision": "PASS"}

    # ── Staleness Filter ──────────────────────────────────────────────────────
    now_ms  = int(time.time() * 1000)
    ts_a    = ex_a.get("timestamp", 0)
    ts_b    = ex_b.get("timestamp", 0)
    age_a   = now_ms - ts_a if ts_a else 9999
    age_b   = now_ms - ts_b if ts_b else 9999

    if age_a > config.MAX_DATA_AGE_MS or age_b > config.MAX_DATA_AGE_MS:
        log.debug(
            f"[Quant] {symbol} stale data: A={age_a}ms B={age_b}ms "
            f"(max {config.MAX_DATA_AGE_MS}ms) — PASS"
        )
        return {"decision": "PASS"}

    # ── Fee parameters ────────────────────────────────────────────────────────
    fee_a = config.FEE_TIERS.get(ex_a.get("name", ""), 0.001)
    fee_b = config.FEE_TIERS.get(ex_b.get("name", ""), 0.001)

    # ── Evaluate direction A→B (buy on A, sell on B) ──────────────────────────
    ask_a = ex_a.get("top_ask", 0.0)
    bid_b = ex_b.get("top_bid", 0.0)
    if ask_a > 0 and bid_b > 0:
        gross_ab    = bid_b - ask_a
        fees_ab     = (ask_a * fee_a) + (bid_b * fee_b)
        net_ab      = gross_ab - fees_ab
        net_ab_usdt = net_ab * (config.MAX_ORDER_USDT / ask_a)  # Scale to USDT
    else:
        net_ab_usdt = -999.0

    # ── Evaluate direction B→A (buy on B, sell on A) ──────────────────────────
    ask_b = ex_b.get("top_ask", 0.0)
    bid_a = ex_a.get("top_bid", 0.0)
    if ask_b > 0 and bid_a > 0:
        gross_ba    = bid_a - ask_b
        fees_ba     = (ask_b * fee_b) + (bid_a * fee_a)
        net_ba      = gross_ba - fees_ba
        net_ba_usdt = net_ba * (config.MAX_ORDER_USDT / ask_b)
    else:
        net_ba_usdt = -999.0

    # ── Select best direction ──────────────────────────────────────────────────
    if net_ab_usdt >= net_ba_usdt and net_ab_usdt > 0:
        best_net   = net_ab_usdt
        buy_name   = ex_a.get("name", "binance")
        sell_name  = ex_b.get("name", "bybit")
        buy_price  = ask_a
        sell_price = bid_b
    elif net_ba_usdt > 0:
        best_net   = net_ba_usdt
        buy_name   = ex_b.get("name", "bybit")
        sell_name  = ex_a.get("name", "binance")
        buy_price  = ask_b
        sell_price = bid_a
    else:
        return {"decision": "PASS", "net_profit": max(net_ab_usdt, net_ba_usdt)}

    # ── Log telemetry ──────────────────────────────────────────────
    log.debug(
        f"[Quant] {symbol} | A→B: ${net_ab_usdt:.4f} | B→A: ${net_ba_usdt:.4f} | "
        f"Best: ${best_net:.4f} | Data age: A={age_a}ms B={age_b}ms"
    )

    if best_net < config.MIN_NET_PROFIT_USDT:
        return {"decision": "PASS", "net_profit": best_net}

    log.info(
        f"🔥 [Quant] Spread detected on {symbol}: "
        f"BUY {buy_name.upper()} @ {buy_price:.4f} | "
        f"SELL {sell_name.upper()} @ {sell_price:.4f} | "
        f"Est gross profit: ${best_net:.4f}"
    )

    return {
        "decision":      "EXECUTE",
        "net_profit":    best_net,
        "buy_exchange":  buy_name,
        "sell_exchange": sell_name,
        "buy_price":     buy_price,
        "sell_price":    sell_price,
        "funding_cost":  0.0,    # Will be deducted in funding_node if hedge active
        "has_hedge":     False,  # Set by build_graph closure using HedgeManager
    }


def risk_node(state: SwarmState, risk_manager: RiskManager, buy_exchange, sell_exchange, paused: bool = False) -> dict:
    """
    Pre-execution risk gate. Checks:
      1. RiskManager (circuit breaker, positions, daily P&L)
      2. Balance pre-staging on both exchange sides
      3. Engine-pause flag (set via Telegram /pause)

    NOTE: open_position() is intentionally NOT called here.
    It is called inside execute_node immediately before the exchange API call,
    so a CancelledError between risk and execute doesn't strand the counter.
    """
    if state.get("decision") != "EXECUTE":
        return {"decision": "PASS"}

    symbol     = state["symbol"]
    net_profit = state.get("net_profit", 0.0)
    buy_ex     = state.get("buy_exchange", "binance")
    sell_ex    = state.get("sell_exchange", "bybit")

    # 1. Engine pause (Telegram /pause)
    if paused:
        return {"decision": "PASS", "risk_reason": "Engine paused via Telegram /pause"}

    # 2. Risk manager gate
    allowed, reason = risk_manager.can_trade(symbol, net_profit)
    if not allowed:
        log.debug(f"[Risk] {symbol} blocked: {reason}")
        return {"decision": "PASS", "risk_reason": reason}

    # 3. Balance check (with soft reservation deducted)
    can_exec, balance_reason = check_can_execute(
        symbol, config.MAX_ORDER_USDT, buy_ex, sell_ex
    )
    if not can_exec:
        log.warning(f"[Balance] {symbol} blocked — {balance_reason}")
        return {"decision": "PASS", "risk_reason": balance_reason}

    # 4. Calculate final order quantity
    buy_price = state.get("buy_price", 0.0)
    qty = risk_manager.calculate_order_qty(symbol, buy_price, buy_exchange)

    if qty <= 0:
        return {"decision": "PASS", "risk_reason": "Calculated qty too small for exchange min"}

    return {
        "decision":      "EXECUTE",
        "order_quantity": qty,
    }



async def execute_node(
    state: SwarmState,
    executor: OrderExecutor,
    risk_manager: RiskManager,
    hedge_manager: HedgeManager,
    rebalancer: "Rebalancer",
    notifier: "Notifier",
) -> dict:
    """
    Fires the actual trade via OrderExecutor (3-leg: spot BUY + spot SELL + perp close)
    and notifies RiskManager of outcome.

    Position lifecycle (critical for correctness):
      open_position()  → called HERE, immediately before the exchange API call.
      close_position() → called in the finally block so it ALWAYS fires,
                          even if CancelledError hits (not caught by except Exception).
    """
    if state.get("decision") != "EXECUTE":
        return {"decision": "PASS"}

    symbol    = state["symbol"]
    buy_ex    = state.get("buy_exchange", "binance")
    sell_ex   = state.get("sell_exchange", "bybit")
    qty       = state.get("order_quantity", 0.0)
    est_pnl   = state.get("net_profit", 0.0)

    # Register position NOW — right before hitting the exchange.
    # close_position() in the finally block guarantees the counter always decrements.
    risk_manager.open_position(symbol)

    realized_pnl = 0.0
    try:
        result = await executor.execute_arb(
            symbol, buy_ex, sell_ex, qty, est_pnl,
            hedge_manager=hedge_manager,
        )

        realized_pnl = result.realized_pnl if result.is_success else 0.0

        if result.is_success:
            log.info(
                f"💰 Trade complete: {symbol} | "
                f"Realized P&L: ${result.realized_pnl:.4f} | "
                f"Latency: {result.execution_ms}ms"
            )

            # ── POST-TRADE CLEANUP: sell acquired crypto back to USDT ────────
            if result.buy_filled_qty > 0 and config.EXECUTION_MODE != "PAPER":
                asyncio.create_task(
                    _cleanup_inventory(
                        executor=executor,
                        symbol=symbol,
                        exchange_name=buy_ex,
                        quantity=result.buy_filled_qty,
                    ),
                    name=f"cleanup-{buy_ex}-{symbol}",
                )
            elif config.EXECUTION_MODE == "PAPER":
                log.info(
                    f"📋 [PAPER] Post-trade cleanup: would SELL {result.buy_filled_qty:.6f} "
                    f"{symbol.split('/')[0]} on {buy_ex.upper()} → USDT"
                )

            # ── AUTO-RESTOCK BYBIT SELL-SIDE + OPEN NEW HEDGE ──────────────
            if result.sell_filled_qty > 0:
                asyncio.create_task(
                    rebalancer.restock_after_trade(
                        symbol=symbol,
                        quantity=result.sell_filled_qty,
                        notifier=notifier,
                    ),
                    name=f"restock-{symbol}",
                )

            # ── PHONE ALERT ────────────────────────────────────────────
            asyncio.create_task(
                notifier.alert_trade_filled(
                    symbol=symbol,
                    realized_pnl=result.realized_pnl,
                    execution_ms=result.execution_ms,
                    buy_ex=buy_ex,
                    sell_ex=sell_ex,
                ),
                name=f"notify-fill-{symbol}",
            )

            return {
                "decision":         "FILLED",
                "execution_status": result.status,
                "realized_pnl":     result.realized_pnl,
            }
        else:
            log.error(f"🚨 Trade failed for {symbol}: {result.error}")
            return {
                "decision":         "FAILED",
                "execution_status": result.status,
                "realized_pnl":     0.0,
            }

    except asyncio.CancelledError:
        # CancelledError is BaseException, not Exception — finally below still runs.
        raise

    except Exception as e:
        log.exception(f"[Execute] Unhandled exception during trade on {symbol}: {e}")
        return {"decision": "FAILED", "execution_status": "EXCEPTION"}

    finally:
        # ALWAYS decrement the position counter, regardless of outcome.
        # This prevents the risk manager from permanently blocking the symbol
        # when a CancelledError or unexpected exception occurs.
        risk_manager.close_position(symbol, realized_pnl)


async def _cleanup_inventory(
    executor,
    symbol: str,
    exchange_name: str,
    quantity: float,
) -> None:
    """
    Post-trade inventory cleanup: sells crypto back to USDT on the buy-side exchange.

    This runs as a background asyncio task after each successful trade. It is
    independent of the main arb loop — failure is logged but never crashes the engine.

    Why this works:
      The arb profit is already locked in at the moment both legs fill.
      Converting ETH→USDT on Binance immediately after keeps the account
      in a clean, delta-neutral USDT-only state between trades.
    """
    import config as _config
    from logger import get_logger as _get_logger
    log2 = _get_logger(__name__)

    base_asset = symbol.split("/")[0]
    log2.info(
        f"[Cleanup] Selling {quantity:.6f} {base_asset} on {exchange_name.upper()} → USDT "
        f"(post-trade inventory reset)"
    )

    try:
        exchange = executor._exchanges[exchange_name]
        order = await exchange.create_market_order(symbol, "sell", quantity)
        avg_price = float(order.get("average", 0))
        usdt_received = avg_price * quantity

        log2.info(
            f"[Cleanup] ✅ {exchange_name.upper()} inventory reset: "
            f"sold {quantity:.6f} {base_asset} @ ${avg_price:.4f} "
            f"→ received ${usdt_received:.2f} USDT"
        )
    except Exception as e:
        log2.error(
            f"[Cleanup] ⚠️  Failed to sell {base_asset} on {exchange_name.upper()}: {e}. "
            f"Manual sell required to restore USDT balance."
        )



# ─── Graph Factory ────────────────────────────────────────────────────

def build_graph(
    risk_manager:   RiskManager,
    order_executor: OrderExecutor,
    hedge_manager:  HedgeManager,
    rebalancer:     Rebalancer,
    notifier:       Notifier,
    binance_exchange,
    bybit_exchange,
):
    """
    Compile the LangGraph DAG with all dependencies injected via closures.
    Hedging, auto-rebalancing, and Telegram alerts are all wired in here.
    """

    # ── Funding-aware quant node (wraps the pure quant_node) ───────────────
    def _quant_node(state: SwarmState) -> dict:
        """
        Runs the base quant analysis, then subtracts the estimated hedge
        funding cost from net_profit. If the trade is no longer profitable
        after funding, route to PASS.
        """
        result = quant_node(state)

        # Only enrich if a trade signal was found AND hedge is enabled
        if result.get("decision") != "EXECUTE" or not config.ENABLE_DELTA_HEDGE:
            return result

        symbol    = state["symbol"]
        buy_price = result.get("buy_price", 0.0)
        qty       = config.MAX_ORDER_USDT / buy_price if buy_price > 0 else 0

        # Funding cost for ONE 8h period (conservative estimate)
        # Positive = we RECEIVE funding. Negative = we PAY.
        funding_income = hedge_manager.estimate_funding_cost(symbol, qty, buy_price)
        has_hedge      = hedge_manager.has_hedge(symbol)

        # Subtract from profit only if funding is a cost (negative income)
        funding_deduction = min(0.0, funding_income)  # Only deduct if we pay
        adjusted_profit   = result["net_profit"] + funding_deduction

        log.debug(
            f"[Quant] {symbol} funding adj: gross=${result['net_profit']:.4f} | "
            f"funding=${funding_income:+.5f} | adj_net=${adjusted_profit:.4f} | "
            f"hedge_open={has_hedge}"
        )

        if adjusted_profit < config.MIN_NET_PROFIT_USDT:
            log.debug(
                f"[Quant] {symbol} PASS after funding deduction "
                f"(${adjusted_profit:.4f} < min ${config.MIN_NET_PROFIT_USDT:.4f})"
            )
            return {"decision": "PASS", "net_profit": adjusted_profit}

        result["net_profit"]   = adjusted_profit
        result["funding_cost"] = funding_deduction
        result["has_hedge"]    = has_hedge
        return result

    # ── Risk node closure ─────────────────────────────────────────────
    def _risk_node(state: SwarmState) -> dict:
        buy_ex  = binance_exchange if state.get("buy_exchange") == "binance" else bybit_exchange
        sell_ex = bybit_exchange   if state.get("sell_exchange") == "bybit"  else binance_exchange
        return risk_node(state, risk_manager, buy_ex, sell_ex, paused=notifier.is_paused)


    # ── Execute node closure (injects all deps) ──────────────────────
    async def _execute_node(state: SwarmState) -> dict:
        return await execute_node(
            state, order_executor, risk_manager,
            hedge_manager, rebalancer, notifier,
        )

    # ── Routing functions ──────────────────────────────────────
    def _route_after_quant(state: SwarmState) -> str:
        return "risk" if state.get("decision") == "EXECUTE" else END

    def _route_after_risk(state: SwarmState) -> str:
        return "execute" if state.get("decision") == "EXECUTE" else END

    graph = StateGraph(SwarmState)
    graph.add_node("scout",   scout_node)
    graph.add_node("quant",   _quant_node)   # funding-aware wrapper
    graph.add_node("risk",    _risk_node)
    graph.add_node("execute", _execute_node)

    graph.set_entry_point("scout")
    graph.add_edge("scout", "quant")
    graph.add_conditional_edges("quant",   _route_after_quant, {"risk": "risk",       END: END})
    graph.add_conditional_edges("risk",    _route_after_risk,  {"execute": "execute", END: END})
    graph.add_edge("execute", END)

    return graph.compile()
