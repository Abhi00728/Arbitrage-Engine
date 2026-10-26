"""
latency_test.py — End-to-end PAPER mode latency benchmark.

Measures time taken at every stage of the arbitrage pipeline:
  Stage 1 : Market data snapshot (memory read)
  Stage 2 : Quant node   (spread calculation + fee adjustment)
  Stage 3 : Risk node    (circuit breaker + balance checks)
  Stage 4 : Execute node (PAPER 3-leg "trade")
  Stage 5 : Cleanup sell (PAPER simulated sell)
  Stage 6 : Auto-restock (PAPER Bybit restock simulation)
  Stage 7 : Telegram alert (fire-and-forget task, measured to task creation)
  TOTAL   : Wall clock from trigger → trade confirmed

Run:
  python3 latency_test.py

No .env needed — all mocks, no API calls.
"""

import asyncio
import time
import sys
import os

# ─── Minimal mock environment (no real exchanges, no .env needed) ─────────────

os.environ.setdefault("EXECUTION_MODE",              "PAPER")
os.environ.setdefault("SCAN_PAIRS",                  "ETH/USDT,BTC/USDT")
os.environ.setdefault("MAX_ORDER_USDT",              "30")
os.environ.setdefault("MIN_NET_PROFIT_USDT",         "0.05")
os.environ.setdefault("MAX_DAILY_LOSS_USDT",         "5")
os.environ.setdefault("MAX_CONCURRENT_POSITIONS",    "3")
os.environ.setdefault("MIN_BALANCE_BUFFER_USDT",     "10")
os.environ.setdefault("MAX_DATA_AGE_MS",             "5000")   # generous for bench
os.environ.setdefault("ENABLE_DELTA_HEDGE",          "true")
os.environ.setdefault("HEDGE_LEVERAGE",              "1.0")
os.environ.setdefault("MAX_FUNDING_RATE_THRESHOLD",  "-0.0005")
os.environ.setdefault("BINANCE_API_KEY",  "test")
os.environ.setdefault("BINANCE_SECRET",   "test")   # FIX: was BINANCE_API_SECRET (doesn't match config.py)
os.environ.setdefault("BYBIT_API_KEY",    "test")
os.environ.setdefault("BYBIT_SECRET",     "test")   # FIX: was BYBIT_API_SECRET
os.environ.setdefault("DB_HOST",                     "localhost")
os.environ.setdefault("DB_PASSWORD",                 "test")
os.environ.setdefault("LOG_LEVEL",                   "WARNING")   # Suppress info noise
os.environ.setdefault("LOG_DIR",                      "/tmp/latency_test_logs")

# ─── Colour helpers ───────────────────────────────────────────────────────────

GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ms(t: float) -> str:
    v = t * 1000
    if v < 1:
        color = GREEN
    elif v < 10:
        color = YELLOW
    else:
        color = "\033[91m"  # red
    return f"{color}{v:.3f}ms{RESET}"

def bar(t: float, scale: float = 50.0) -> str:
    """ASCII bar proportional to time."""
    n = max(1, int(t * 1000 * scale / 100))
    return f"{'█' * min(n, 60)}"

# ─── Stage Timers ─────────────────────────────────────────────────────────────

timings: dict = {}

def record(stage: str, elapsed: float):
    timings[stage] = elapsed

# ─── Mock exchange objects ─────────────────────────────────────────────────────

class MockExchange:
    """Mimics CCXT Pro interface, returns instant results with realistic latency."""

    def __init__(self, name: str, latency_ms: float = 5):
        self.name     = name
        self._delay   = latency_ms / 1000
        self.markets  = {}

    async def create_market_order(self, symbol, side, qty):
        await asyncio.sleep(self._delay)
        base_price = 3500.0 if "ETH" in symbol else 65000.0
        slippage   = 0.0001 * (1 if side == "sell" else -1)
        price      = base_price * (1 + slippage)
        return {
            "id":       f"mock-{side}-{int(time.time()*1000)}",
            "status":   "closed",
            "filled":   qty,
            "average":  price,
            "cost":     price * qty,
        }

    async def fetch_balance(self):
        return {"free": {"USDT": 500.0, "ETH": 0.5, "BTC": 0.01}}

    def market(self, symbol):
        return {"precision": {"amount": 4}, "limits": {"amount": {"min": 0.001}}}

    def amount_to_precision(self, symbol, qty):
        return f"{qty:.4f}"


# ─── Populate fake market data ─────────────────────────────────────────────────

def seed_market_data():
    from market_feed import LIVE_MARKET_MATRIX
    import time as _t
    now = int(_t.time() * 1000)

    # Inject a realistic spread: Bybit ask > Binance bid → arb opportunity
    LIVE_MARKET_MATRIX["ETH/USDT"] = {
        "exchange_a": {           # Binance — BUY here (cheaper)
            "name":      "binance",
            "top_bid":   3488.00,
            "top_ask":   3490.00,   # ← taker buy price
            "timestamp": now,
        },
        "exchange_b": {           # Bybit  — SELL here (more expensive)
            "name":      "bybit",
            "top_bid":   3510.00,   # ← taker sell price ($20 spread)
            "top_ask":   3512.00,
            "timestamp": now,
        },
    }

    # Also seed balance monitor so risk node doesn't block
    from balance_monitor import LIVE_BALANCES, _last_fetch_ts
    LIVE_BALANCES["binance"] = {"USDT": 200.0, "ETH": 0.1}
    LIVE_BALANCES["bybit"]   = {"USDT": 150.0, "ETH": 0.5}
    _last_fetch_ts["binance"] = _t.time()
    _last_fetch_ts["bybit"]   = _t.time()


# ─── Individual Stage Benchmarks ──────────────────────────────────────────────

async def bench_market_snapshot(symbol: str) -> dict:
    t0 = time.perf_counter()
    from market_feed import LIVE_MARKET_MATRIX
    data = LIVE_MARKET_MATRIX.get(symbol, {})
    record("1_market_snapshot", time.perf_counter() - t0)
    return data


async def bench_quant(state: dict, binance, bybit) -> dict:
    t0 = time.perf_counter()
    from quant_engine import quant_node, scout_node
    scouted = scout_node(state)   # returns updated state dict with symbol + market_data
    merged  = {**state, **scouted} # merge so symbol is present for quant_node
    result  = quant_node(merged)
    record("2_quant_node", time.perf_counter() - t0)
    return {**merged, **result}   # return full merged state


async def bench_risk(state: dict, risk_manager, binance, bybit) -> dict:
    from quant_engine import risk_node
    t0 = time.perf_counter()
    result = risk_node(state, risk_manager, binance, bybit)
    record("3_risk_node", time.perf_counter() - t0)
    return result


async def bench_execute(state: dict, executor, risk_manager, hedge_manager) -> dict:
    t0 = time.perf_counter()
    from order_executor import TradeResult
    result = await executor.execute_arb(
        symbol        = state["symbol"],
        buy_exchange  = state.get("buy_exchange", "binance"),
        sell_exchange = state.get("sell_exchange", "bybit"),
        quantity      = state.get("order_quantity", 0.008),
        estimated_pnl = state.get("net_profit", 0.1),
        hedge_manager = hedge_manager,
    )
    record("4_execute_3leg", time.perf_counter() - t0)
    return result


async def bench_cleanup(executor, symbol: str, qty: float) -> None:
    t0 = time.perf_counter()
    # In paper mode _cleanup_inventory just logs, so simulate it
    await asyncio.sleep(0.001)
    record("5_cleanup_sell", time.perf_counter() - t0)


async def bench_restock(rebalancer, symbol: str, qty: float) -> None:
    t0 = time.perf_counter()
    await rebalancer.restock_after_trade(symbol=symbol, quantity=qty)
    record("6_restock", time.perf_counter() - t0)


async def bench_telegram_task_creation(notifier, symbol: str) -> None:
    t0 = time.perf_counter()
    # Just measure task creation overhead (no real HTTP in test)
    asyncio.create_task(asyncio.sleep(0))  # mock the notify fire
    record("7_telegram_dispatch", time.perf_counter() - t0)


# ─── Full End-to-End Run ──────────────────────────────────────────────────────

async def run_benchmark(n_runs: int = 10):
    print(f"\n{BOLD}{CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{BOLD}{CYAN}  QuantSwarm PAPER Latency Benchmark{RESET}")
    print(f"{BOLD}{CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}\n")

    # ── Setup mocks ───────────────────────────────────────────────────────────
    binance = MockExchange("binance", latency_ms=5)
    bybit   = MockExchange("bybit",   latency_ms=6)

    from risk_manager import RiskManager
    from order_executor import OrderExecutor
    from hedge_manager import HedgeManager
    from rebalancer import Rebalancer
    from notifier import Notifier

    risk_manager   = RiskManager()
    order_executor = OrderExecutor(binance, bybit, db_pool=None)
    hedge_manager  = HedgeManager(bybit)
    rebalancer     = Rebalancer(bybit, hedge_manager)
    notifier       = Notifier()

    seed_market_data()

    symbol = "ETH/USDT"
    all_runs = []

    print(f"  Running {n_runs} iterations (PAPER mode, simulated {5}ms exchange RTT)\n")

    for i in range(n_runs):
        timings.clear()
        risk_manager._open_positions.clear()   # Reset between runs
        seed_market_data()   # Refresh timestamps so staleness filter never fires
        global_t0 = time.perf_counter()

        # Stage 1: market snapshot
        await bench_market_snapshot(symbol)

        # Stage 2: quant
        state = {"symbol": symbol, "market_data": {}, "decision": "PASS",
                 "net_profit": 0.0, "order_quantity": 0.0,
                 "risk_blocked": False, "risk_reason": "",
                 "execution_status": "", "realized_pnl": 0.0}
        quant_result = await bench_quant(state, binance, bybit)
        state.update(quant_result)

        if state.get("decision") != "EXECUTE":
            print(f"  Run {i+1}: ⚠️  No arb signal detected — check seed data")
            continue

        # Stage 3: risk
        risk_result = await bench_risk(state, risk_manager, binance, bybit)
        state.update(risk_result)

        if state.get("decision") != "EXECUTE":
            print(f"  Run {i+1}: ⚠️  Risk blocked: {state.get('risk_reason')}")
            risk_manager._open_positions.clear()
            continue

        # Stage 4: execute (3-leg PAPER)
        trade_result = await bench_execute(state, order_executor, risk_manager, hedge_manager)
        risk_manager.close_position(symbol, trade_result.realized_pnl)

        # Stage 5: cleanup sell
        await bench_cleanup(order_executor, symbol, trade_result.buy_filled_qty)

        # Stage 6: restock
        await bench_restock(rebalancer, symbol, trade_result.sell_filled_qty)

        # Stage 7: telegram task dispatch
        await bench_telegram_task_creation(notifier, symbol)

        total = time.perf_counter() - global_t0
        timings["TOTAL"] = total
        all_runs.append(dict(timings))

    if not all_runs:
        print("  ❌ No successful runs. Check seed data.")
        return

    # ── Print results ─────────────────────────────────────────────────────────
    stages = [k for k in all_runs[0].keys()]
    print(f"  {'Stage':<28} {'Avg':>10}  {'Min':>10}  {'Max':>10}  Bar")
    print(f"  {'─'*28} {'─'*10}  {'─'*10}  {'─'*10}  {'─'*30}")

    labels = {
        "1_market_snapshot":   "1. Market snapshot",
        "2_quant_node":        "2. Quant (spread calc)",
        "3_risk_node":         "3. Risk checks",
        "4_execute_3leg":      "4. Execute (3-leg PAPER)",
        "5_cleanup_sell":      "5. Cleanup sell (async)",
        "6_restock":           "6. Auto-restock Bybit",
        "7_telegram_dispatch": "7. Telegram dispatch",
        "TOTAL":               "★ TOTAL end-to-end",
    }

    for stage in stages:
        vals = [r[stage] for r in all_runs if stage in r]
        avg  = sum(vals) / len(vals)
        mn   = min(vals)
        mx   = max(vals)
        label = labels.get(stage, stage)
        sep = "─" * 60 if stage == "TOTAL" else ""
        if sep:
            print(f"  {sep}")
        print(f"  {label:<28} {ms(avg):>18}  {ms(mn):>18}  {ms(mx):>18}  {bar(avg)}")

    total_avg = sum(r["TOTAL"] for r in all_runs) / len(all_runs)
    print(f"\n  Runs: {len(all_runs)}/{n_runs} successful\n")

    # ── Key insight breakdown ─────────────────────────────────────────────────
    print(f"{BOLD}{CYAN}  Key Insight: Where does time go?{RESET}")

    exec_avg = sum(r.get("4_execute_3leg", 0) for r in all_runs) / len(all_runs)
    quant_avg = sum(r.get("2_quant_node", 0) for r in all_runs) / len(all_runs)
    overhead  = total_avg - exec_avg

    print(f"  Exchange RTT dominates: {ms(exec_avg)} ({exec_avg/total_avg*100:.0f}% of total)")
    print(f"  Engine logic overhead:  {ms(overhead)} ({overhead/total_avg*100:.0f}% of total)")
    print(f"  Quant calc alone:       {ms(quant_avg)} (pure Python math)")
    print()
    print(f"  {GREEN}In LIVE mode, both spot legs + perp close fire simultaneously{RESET}")
    print(f"  {GREEN}— total latency ≈ slowest single exchange RTT, not the sum.{RESET}")
    print(f"  {GREEN}Typical live RTT to Binance/Bybit from India: 50–120ms.{RESET}")
    print(f"\n{BOLD}{CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}\n")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    asyncio.run(run_benchmark(n_runs=n))
