"""
balance_monitor.py — Pre-staged balance checker (latency solution).

This module directly solves the cross-exchange transfer latency problem.

The core insight:
  Crypto transfers between exchanges take 1-30 minutes.
  We cannot transfer funds during an arbitrage opportunity (it will be gone).
  Solution: Keep pre-staged buffers on BOTH exchanges at all times.
  This module checks those balances before every trade and gates execution.

Responsibilities:
  - Poll Binance and Bybit balances every 30 seconds (REST, not WebSocket)
  - Expose check_can_execute(symbol, usdt_amount) for the risk layer to call
  - Log warnings when either side drops below MIN_BALANCE_BUFFER_USDT
  - Track balance snapshots in the DB for audit purposes
"""

import asyncio
import time
from typing import Dict, Tuple, Optional

import config
from logger import get_logger, log_trade_event

log = get_logger(__name__)

# ─── Shared Balance State ────────────────────────────────────────────────────

LIVE_BALANCES: Dict[str, Dict[str, float]] = {
    "binance": {},  # {"USDT": 500.0, "ETH": 0.5, ...}
    "bybit":   {},
}

_last_fetch_ts: Dict[str, float] = {
    "binance": 0.0,
    "bybit":   0.0,
}

BALANCE_POLL_INTERVAL_S: int = 30  # seconds between balance refreshes

# ─── Soft Balance Reservations ────────────────────────────────────────────────────────
# When N workers all see sufficient balance, they could all try to trade
# simultaneously, leading to InsufficientFunds on the exchange. Soft
# reservations are subtracted from the available balance during check_can_execute.
# They are released when execute_node finishes (success or failure).

_RESERVED_USDT: Dict[str, float] = {"binance": 0.0, "bybit": 0.0}
_RESERVED_BASE: Dict[str, Dict[str, float]] = {"binance": {}, "bybit": {}}


def reserve_trade_budget(
    buy_exchange: str,
    sell_exchange: str,
    usdt_amount: float,
    base_asset: str,
    base_amount: float,
) -> None:
    """Soft-reserve funds before a trade fires. Call after passing check_can_execute."""
    _RESERVED_USDT[buy_exchange]  = _RESERVED_USDT.get(buy_exchange, 0.0)  + usdt_amount
    assets = _RESERVED_BASE.setdefault(sell_exchange, {})
    assets[base_asset] = assets.get(base_asset, 0.0) + base_amount


def release_trade_budget(
    buy_exchange: str,
    sell_exchange: str,
    usdt_amount: float,
    base_asset: str,
    base_amount: float,
) -> None:
    """Release soft reservation after a trade completes or fails."""
    _RESERVED_USDT[buy_exchange]  = max(0.0, _RESERVED_USDT.get(buy_exchange, 0.0)  - usdt_amount)
    assets = _RESERVED_BASE.get(sell_exchange, {})
    assets[base_asset] = max(0.0, assets.get(base_asset, 0.0) - base_amount)



# ─── Balance Fetch ────────────────────────────────────────────────────────────

async def _fetch_balance(exchange, exchange_name: str) -> None:
    """Fetch and update free balances for a single exchange via REST."""
    try:
        result = await exchange.fetch_balance()
        free = result.get("free", {})
        # Filter to only non-zero balances to avoid noise
        LIVE_BALANCES[exchange_name] = {
            asset: float(amount)
            for asset, amount in free.items()
            if float(amount or 0) > 0
        }
        _last_fetch_ts[exchange_name] = time.time()

        log.debug(
            f"[Balance] {exchange_name.upper()} snapshot updated",
            extra={
                "exchange": exchange_name,
                "balances": LIVE_BALANCES[exchange_name],
            },
        )

        # Warn if USDT is critically low
        usdt_free = LIVE_BALANCES[exchange_name].get("USDT", 0.0)
        if usdt_free < config.MIN_BALANCE_BUFFER_USDT:
            log.warning(
                f"[Balance] ⚠️  {exchange_name.upper()} USDT below buffer! "
                f"Free: ${usdt_free:.2f} | Buffer: ${config.MIN_BALANCE_BUFFER_USDT:.2f} — "
                f"Manual rebalance required."
            )

    except Exception as e:
        log.error(f"[Balance] Failed to fetch balance from {exchange_name}: {e}")


# ─── Background Monitor ───────────────────────────────────────────────────────

class BalanceMonitor:
    """
    Background task that periodically polls exchange balances.
    Acts as the guardian that enables or blocks trade execution.
    """

    def __init__(self, binance_exchange, bybit_exchange):
        self._exchanges = {
            "binance": binance_exchange,
            "bybit":   bybit_exchange,
        }
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def start(self) -> asyncio.Task:
        self._task = asyncio.create_task(self._poll_loop(), name="balance-monitor")
        log.info("[Balance] Monitor started (polling every 30s)")
        return self._task

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        log.info("[Balance] Monitor stopped")

    async def fetch_once(self) -> None:
        """Fetch balances from both exchanges immediately (blocking, one-shot)."""
        await asyncio.gather(
            _fetch_balance(self._exchanges["binance"], "binance"),
            _fetch_balance(self._exchanges["bybit"],   "bybit"),
        )

    async def _poll_loop(self) -> None:
        """Continuously fetch balances with a 30-second interval."""
        while not self._stop_event.is_set():
            await asyncio.gather(
                _fetch_balance(self._exchanges["binance"], "binance"),
                _fetch_balance(self._exchanges["bybit"],   "bybit"),
            )
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=BALANCE_POLL_INTERVAL_S,
                )
            except asyncio.TimeoutError:
                pass  # Expected — loop again



# ─── Execution Gate ───────────────────────────────────────────────────────────

def check_can_execute(
    symbol: str,
    usdt_amount: float,
    buy_exchange: str = "binance",
    sell_exchange: str = "bybit",
) -> Tuple[bool, str]:
    """
    Check if pre-staged balances are sufficient for both legs of an arb trade.

    Args:
        symbol:        Trading pair e.g. "ETH/USDT"
        usdt_amount:   USDT value of each leg (e.g., 100.0)
        buy_exchange:  Exchange where we BUY the base asset (needs USDT)
        sell_exchange: Exchange where we SELL the base asset (needs base asset)

    Returns:
        (can_execute: bool, reason: str)
    """
    base_asset = symbol.split("/")[0]  # e.g., "ETH" from "ETH/USDT"

    # ── Check buyer side: needs USDT ─────────────────────────────────────────────────
    buy_balances   = LIVE_BALANCES.get(buy_exchange, {})
    usdt_available = buy_balances.get("USDT", 0.0)
    reserved_usdt  = _RESERVED_USDT.get(buy_exchange, 0.0)
    effective_usdt = usdt_available - reserved_usdt  # subtract in-flight reservations

    required_usdt = usdt_amount + config.MIN_BALANCE_BUFFER_USDT
    if effective_usdt < required_usdt:
        reason = (
            f"{buy_exchange.upper()} USDT insufficient: "
            f"have ${effective_usdt:.2f} (${usdt_available:.2f} - ${reserved_usdt:.2f} reserved), "
            f"need ${required_usdt:.2f} (trade ${usdt_amount:.2f} + buffer ${config.MIN_BALANCE_BUFFER_USDT:.2f})"
        )
        return False, reason

    # ── Check seller side: needs base asset ────────────────────────────────────────────
    sell_balances  = LIVE_BALANCES.get(sell_exchange, {})
    base_available = sell_balances.get(base_asset, 0.0)
    reserved_base  = _RESERVED_BASE.get(sell_exchange, {}).get(base_asset, 0.0)
    effective_base = base_available - reserved_base  # subtract in-flight reservations

    # Use top_bid (the actual sell price) — not top_ask — to estimate base qty needed.
    # Using ask would underestimate required_base by ~spread amount.
    from market_feed import LIVE_MARKET_MATRIX
    market_data = LIVE_MARKET_MATRIX.get(symbol, {})
    sell_side   = market_data.get("exchange_b", {}) if sell_exchange == "bybit" else market_data.get("exchange_a", {})
    bid_price   = sell_side.get("top_bid", 0.0)   # FIX: was top_ask (wrong side)

    if bid_price > 0:
        required_base = usdt_amount / bid_price
    else:
        # No price data yet — cannot safely estimate, block trade
        return False, f"No price data for {symbol} on {sell_exchange} to estimate required {base_asset}"

    if effective_base < required_base:
        reason = (
            f"{sell_exchange.upper()} {base_asset} insufficient: "
            f"have {effective_base:.6f} ({base_available:.6f} - {reserved_base:.6f} reserved), "
            f"need ~{required_base:.6f} (worth ~${usdt_amount:.2f})"
        )
        return False, reason


    # ── Check balance data freshness ──────────────────────────────────────────
    now = time.time()
    age_buy  = now - _last_fetch_ts.get(buy_exchange, 0)
    age_sell = now - _last_fetch_ts.get(sell_exchange, 0)
    if age_buy > 120 or age_sell > 120:  # Data older than 2 minutes
        return False, "Balance data stale (>120s) — skipping trade until refresh"

    return True, "OK"


def get_balance_snapshot() -> Dict:
    """Return a copy of current live balances for logging."""
    return {k: dict(v) for k, v in LIVE_BALANCES.items()}
