"""
market_feed.py — Real-time WebSocket market feed manager.

Maintains persistent WebSocket connections to Binance and Bybit,
continuously updating the shared LIVE_MARKET_MATRIX with the latest
orderbook top-of-book data. Puts a signal onto MARKET_EVENT_QUEUE
whenever data changes, triggering the quant engine.

Key design decisions:
  - Exponential backoff on reconnect (avoids hammering exchange on outages)
  - Deduplication: only queues a symbol if it isn't already queued/pending
  - Captures top-of-book AND depth (size at best bid/ask) for slippage estimation
  - Timestamps preserved from exchange server time (used for staleness check)
"""

import asyncio
import time
from typing import Dict, Any

import config
from logger import get_logger

log = get_logger(__name__)

# ─── Shared State (read by quant engine) ─────────────────────────────────────

LIVE_MARKET_MATRIX: Dict[str, Dict[str, Any]] = {
    pair: {
        "exchange_a": {},   # Binance
        "exchange_b": {},   # Bybit
    }
    for pair in config.SCAN_PAIRS
}

# Set of symbols currently in the queue (prevent flooding with redundant signals)
_QUEUED_SYMBOLS: set = set()

# Event queue — quant engine consumes from this
MARKET_EVENT_QUEUE: asyncio.Queue = asyncio.Queue()

# ─── Feed Worker ──────────────────────────────────────────────────────────────

async def _watch_exchange_pair(
    exchange,
    symbol: str,
    exchange_key: str,
    stop_event: asyncio.Event,
) -> None:
    """
    Persistent WebSocket worker for a single (exchange, symbol) pair.
    Reconnects with exponential backoff on any failure.
    """
    backoff = 1   # seconds
    max_backoff = 30

    log.info(f"[Feed] Starting stream: {exchange.id.upper()} / {symbol}")

    while not stop_event.is_set():
        try:
            # Bybit spot WS only accepts limit in [1, 50, 200, 1000].
            # Use 1 (top-of-book only) — cheapest and sufficient for arb.
            limit = 1 if exchange.id == "bybit" else 5
            orderbook = await exchange.watch_order_book(symbol, limit=limit)

            bids = orderbook.get("bids", [])
            asks = orderbook.get("asks", [])

            if not bids or not asks:
                continue

            top_bid,     bid_size = bids[0][0], bids[0][1]
            top_ask,     ask_size = asks[0][0], asks[0][1]
            server_ts = orderbook.get("timestamp") or int(time.time() * 1000)

            LIVE_MARKET_MATRIX[symbol][exchange_key] = {
                "name":      exchange.id,
                "top_bid":   top_bid,
                "top_ask":   top_ask,
                "bid_size":  bid_size,
                "ask_size":  ask_size,
                "timestamp": server_ts,
            }

            # Deduplicated queue push — skip if already pending
            if symbol not in _QUEUED_SYMBOLS:
                _QUEUED_SYMBOLS.add(symbol)
                await MARKET_EVENT_QUEUE.put(symbol)

            # Reset backoff on successful tick
            backoff = 1

        except asyncio.CancelledError:
            log.info(f"[Feed] Stream cancelled: {exchange.id.upper()} / {symbol}")
            break

        except Exception as e:
            log.warning(
                f"[Feed] Stream error: {exchange.id.upper()} / {symbol}: {e} "
                f"— reconnecting in {backoff}s"
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


# ─── MarketFeedManager ────────────────────────────────────────────────────────

class MarketFeedManager:
    """
    Manages all (exchange, symbol) WebSocket goroutines.
    Creates one task per (exchange × pair) combination.
    """

    def __init__(self, binance_exchange, bybit_exchange):
        self.exchanges = {
            "exchange_a": binance_exchange,
            "exchange_b": bybit_exchange,
        }
        self._tasks = []
        self._stop_event = asyncio.Event()

    def start(self) -> list:
        """
        Spawn all feed worker tasks and return the task list.
        Caller should include these in the main asyncio.gather().
        """
        for pair in config.SCAN_PAIRS:
            for key, exchange in self.exchanges.items():
                task = asyncio.create_task(
                    _watch_exchange_pair(exchange, pair, key, self._stop_event),
                    name=f"feed-{exchange.id}-{pair}",
                )
                self._tasks.append(task)

        log.info(
            f"[Feed] Launched {len(self._tasks)} stream workers "
            f"({len(config.SCAN_PAIRS)} pairs × 2 exchanges)"
        )
        return self._tasks

    async def stop(self) -> None:
        """Signal all feed workers to stop and cancel their tasks."""
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        log.info("[Feed] All stream workers stopped")


def mark_symbol_dequeued(symbol: str) -> None:
    """
    Call this after the quant engine finishes processing a symbol tick.
    Removes it from the dedup set so future ticks can be pushed again.
    """
    _QUEUED_SYMBOLS.discard(symbol)
