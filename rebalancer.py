"""
rebalancer.py — Automatic post-trade inventory restocking.

Solves the "who restocks Bybit?" problem completely automatically.

The Self-Sustaining Cycle:
  After every arb trade, both exchanges end up holding USDT.
  The rebalancer detects this and automatically:
    1. Buys base asset on Bybit spot (using the USDT just received from the sell leg)
    2. Opens a new hedge short on Bybit perp for the purchased amount

  Everything happens on-exchange (< 2 seconds). No blockchain transfers.
  No manual action required. The engine resets itself between every trade.

Full deterministic cycle:
  PRE-TRADE:  Binance[$USDT] + Bybit[ETH spot + SHORT perp]  → net delta = 0
  TRADE FIRES: 4-leg execution (BUY + SELL + HEDGE CLOSE + CLEANUP SELL)
  POST-TRADE: Binance[$USDT] + Bybit[$USDT]                  → engine idle
  REBALANCE:  Bybit buys ETH + opens SHORT perp               → back to PRE-TRADE

The rebalancer runs as a background task triggered by quant_engine after each
successful trade. It never blocks the main execution loop.
"""

import asyncio
import time
from typing import Optional

import config
from logger import get_logger, log_trade_event
from hedge_manager import HedgeManager

log = get_logger(__name__)


class Rebalancer:
    """
    Automatically restocks the sell-side exchange after each completed trade.
    Runs as a background asyncio task — never blocks the quant loop.
    """

    def __init__(self, bybit_exchange, hedge_manager: HedgeManager):
        self._bybit        = bybit_exchange
        self._hedge        = hedge_manager
        self._in_progress  = set()     # Symbols currently being restocked (debounce)
        self._last_restock = {}        # symbol → timestamp of last restock

    async def restock_after_trade(
        self,
        symbol:   str,
        quantity: float,
        notifier=None,     # Optional Notifier for phone alert
    ) -> None:
        """
        Called after a successful trade fills to restock Bybit's sell-side inventory.

        Steps:
          1. Buy `quantity` of base asset on Bybit spot (uses USDT just received)
          2. Open a new SHORT perp hedge for the same quantity
          3. Notify via Telegram if notifier is configured

        This is idempotent: if a restock is already in progress for this symbol,
        the duplicate call is ignored.

        Args:
            symbol:   Trading pair, e.g. "ETH/USDT"
            quantity: Amount of base asset to restock (same as what was just sold)
            notifier: Optional Notifier instance for phone alerts
        """
        if symbol in self._in_progress:
            log.debug(f"[Rebalancer] Restock already in progress for {symbol} — skipping")
            return

        self._in_progress.add(symbol)

        try:
            base_asset = symbol.split("/")[0]

            log.info(
                f"[Rebalancer] 🔄 Starting auto-restock for {symbol}: "
                f"buying {quantity:.6f} {base_asset} on Bybit spot + opening hedge..."
            )

            if config.EXECUTION_MODE == "PAPER":
                await self._paper_restock(symbol, quantity, base_asset, notifier)
                return

            # ── Step 1: Buy base asset on Bybit spot ──────────────────────────
            buy_order = await self._buy_spot_on_bybit(symbol, quantity, base_asset)
            if not buy_order:
                return  # Error already logged

            actual_qty   = float(buy_order.get("filled",  quantity))
            actual_price = float(buy_order.get("average", 0.0))

            # ── Step 2: Open hedge short immediately after spot buy ────────────
            hedge_ok = await self._hedge.open_hedge(symbol, actual_qty, actual_price)

            # ── Step 3: Log and notify ────────────────────────────────────────
            if hedge_ok:
                msg = (
                    f"🔄 Auto-restock complete: {symbol}\n"
                    f"  Bought {actual_qty:.6f} {base_asset} @ ${actual_price:.4f}\n"
                    f"  Hedge SHORT opened ✅\n"
                    f"  Engine ready for next arb."
                )
                log.info(f"[Rebalancer] ✅ {symbol} restocked and hedged. Ready.")
                if notifier:
                    await notifier.send(msg)
            else:
                msg = (
                    f"⚠️ Partial restock: {symbol}\n"
                    f"  Bought {actual_qty:.6f} {base_asset} @ ${actual_price:.4f}\n"
                    f"  Hedge open FAILED — spot is unhedged!\n"
                    f"  Check Bybit and open hedge manually."
                )
                log.warning(f"[Rebalancer] ⚠️  {symbol} bought but hedge FAILED")
                if notifier:
                    await notifier.send(msg, level="warning")

            self._last_restock[symbol] = time.time()

        except Exception as e:
            log.error(f"[Rebalancer] Auto-restock failed for {symbol}: {e}")
            if notifier:
                await notifier.send(
                    f"🚨 Auto-restock FAILED for {symbol}: {e}\n"
                    f"Manual action may be needed.",
                    level="error"
                )
        finally:
            self._in_progress.discard(symbol)

    # ─── Bybit Spot Buy ───────────────────────────────────────────────────────

    async def _buy_spot_on_bybit(self, symbol: str, quantity: float, base_asset: str) -> Optional[dict]:
        """Place a market buy order on Bybit spot to restock inventory."""
        try:
            order = await self._bybit.create_market_order(symbol, "buy", quantity)
            log.info(
                f"[Rebalancer] Bybit spot BUY: {quantity:.6f} {base_asset} | "
                f"filled={order.get('filled')} avg=${order.get('average'):.4f}"
            )
            return order
        except Exception as e:
            log.error(
                f"[Rebalancer] Bybit spot buy FAILED for {symbol}: {e}. "
                f"Bybit sell-side inventory is depleted — engine will idle for this pair."
            )
            return None

    # ─── Paper Simulation ─────────────────────────────────────────────────────

    async def _paper_restock(
        self, symbol: str, quantity: float, base_asset: str, notifier=None
    ) -> None:
        """Simulate restock in PAPER mode."""
        await asyncio.sleep(0.01)  # Simulate order latency

        # FIX: pull real current price so paper P&L simulation is accurate.
        # Previously hardcoded price=0.0 made every paper hedge show wrong P&L.
        from market_feed import LIVE_MARKET_MATRIX
        market_data = LIVE_MARKET_MATRIX.get(symbol, {})
        ex_b = market_data.get("exchange_b", {})
        # Use ask price for the BUY we're simulating (what we'd pay)
        paper_price = ex_b.get("top_ask") or ex_b.get("top_bid") or 0.0

        self._hedge._paper_open_hedge(symbol, f"{base_asset}/USDT:USDT", quantity, price=paper_price)
        msg = (
            f"📋 [PAPER] Auto-restock simulated: {symbol}\n"
            f"  Would buy {quantity:.6f} {base_asset} on Bybit\n"
            f"  Would open SHORT hedge ✅"
        )
        log.info(f"[Rebalancer] 📋 {msg}")
        if notifier:
            await notifier.send(msg)

        self._last_restock[symbol] = time.time()

    def restock_status(self) -> dict:
        """Return last restock timestamps per symbol."""
        now = time.time()
        return {
            sym: f"{int(now - ts)}s ago"
            for sym, ts in self._last_restock.items()
        }
