"""
notifier.py — Telegram Bot for phone alerts and remote control.

Gives you live visibility into the engine from your phone, 24/7.

Features:
  Alerts (pushed to you automatically):
    🔥 Arb opportunity detected
    💰 Trade filled with realized P&L
    🔄 Auto-restock complete
    ⚠️  Balance warnings
    🚨 Critical errors (imbalances, circuit breaker trips)
    📊 Hourly P&L summary

  Commands (you send from phone):
    /status   → live balances + daily P&L + open hedges
    /pause    → pause the engine (no new trades)
    /resume   → resume the engine
    /hedges   → list all open hedge positions
    /help     → show all commands

Setup (one-time, takes 2 minutes):
  1. Message @BotFather on Telegram → /newbot → copy the TOKEN
  2. Message your new bot once, then visit:
     https://api.telegram.org/bot<TOKEN>/getUpdates
     Copy your chat_id from the response
  3. Add to .env:
     TELEGRAM_BOT_TOKEN=your_token_here
     TELEGRAM_CHAT_ID=your_chat_id_here

Notes:
  - Uses plain HTTP polling (no webhook needed — works behind any NAT/firewall)
  - If TELEGRAM_BOT_TOKEN is not set, notifier silently no-ops (no crash)
  - All outgoing messages are rate-limited to prevent Telegram API bans
"""

import asyncio
import time
from typing import Optional

import aiohttp

import config
from logger import get_logger

log = get_logger(__name__)


# Legacy module-level flag kept as a no-op shim to avoid import errors
# in any code that still references notifier.ENGINE_PAUSED directly.
# Real state lives in Notifier._paused — use notifier.is_paused.
ENGINE_PAUSED: bool = False  # Always False; kept only for backwards compat.


class Notifier:
    """
    Telegram bot that sends alerts and accepts remote commands.
    One instance shared engine-wide. Non-blocking — all sends are fire-and-forget.
    """

    def __init__(self, risk_manager=None, rebalancer=None, balance_monitor=None):
        self._token    = config.TELEGRAM_BOT_TOKEN
        self._chat_id  = config.TELEGRAM_CHAT_ID
        self._enabled  = bool(self._token and self._chat_id)
        self._risk_mgr = risk_manager
        self._rebal    = rebalancer
        self._balmon   = balance_monitor
        self._update_offset = 0     # Telegram polling offset

        # Engine pause state — instance attribute, not a module-level global.
        # Read via notifier.is_paused; set via notifier.pause()/notifier.resume().
        self._paused: bool = False

        # Non-blocking rate-limited sender.
        # send() puts messages on this queue and returns immediately.
        # _rate_limited_sender() drains the queue at ≤1 msg/s in the background.
        self._send_queue: asyncio.Queue = asyncio.Queue(maxsize=50)
        self._sender_task: Optional[asyncio.Task] = None

        if self._enabled:
            log.info(
                f"[Notifier] Telegram bot configured ✅ | "
                f"chat_id={self._chat_id[:6]}..."
            )
        else:
            log.info("[Notifier] Telegram not configured — phone alerts disabled")

    # ─── Pause State ─────────────────────────────────────────────────────────

    @property
    def is_paused(self) -> bool:
        """True while the engine is paused via /pause Telegram command."""
        return self._paused

    def pause(self, sender: str = "system") -> None:
        self._paused = True
        log.warning(f"[Notifier] Engine PAUSED by {sender}")

    def resume(self, sender: str = "system") -> None:
        self._paused = False
        log.info(f"[Notifier] Engine RESUMED by {sender}")


    # ─── Non-Blocking Message Sending ────────────────────────────────────────

    async def send(self, text: str, level: str = "info") -> None:
        """
        Enqueue a Telegram message. Returns immediately — NEVER blocks.

        A background _rate_limited_sender() task drains the queue at ≤1 msg/s,
        so a Telegram alert never delays the hot trade-execution path.
        """
        if not self._enabled:
            return

        # Lazily start the background sender task (needs running event loop)
        if self._sender_task is None or self._sender_task.done():
            self._sender_task = asyncio.create_task(
                self._rate_limited_sender(), name="tg-rate-sender"
            )

        try:
            self._send_queue.put_nowait(text)
        except asyncio.QueueFull:
            log.warning("[Notifier] Send queue full — dropping low-priority message")

    async def _rate_limited_sender(self) -> None:
        """Background task: drains _send_queue at ≤1 message/second."""
        while True:
            text = await self._send_queue.get()
            try:
                await self._send_message(text)
            except Exception as e:
                log.warning(f"[Notifier] Sender error: {e}")
            finally:
                self._send_queue.task_done()
            await asyncio.sleep(1.1)  # ~1 msg/s per Telegram chat


    async def _send_message(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        payload = {
            "chat_id":    self._chat_id,
            "text":       text,
            "parse_mode": "Markdown",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status != 200:
                        log.warning(f"[Notifier] Telegram send failed: HTTP {resp.status}")
        except Exception as e:
            log.warning(f"[Notifier] Telegram send error: {e}")

    # ─── Command Polling ──────────────────────────────────────────────────────

    async def start_polling(self, stop_event: asyncio.Event) -> None:
        """
        Poll Telegram for incoming commands.
        Long-polls every 3 seconds. Handles /status /pause /resume /hedges /help.
        """
        if not self._enabled:
            return

        # Ensure background sender is running before first message
        if self._sender_task is None or self._sender_task.done():
            self._sender_task = asyncio.create_task(
                self._rate_limited_sender(), name="tg-rate-sender"
            )

        log.info("[Notifier] Starting Telegram command polling...")
        await self.send(
            f"🤖 *QuantSwarm Engine Online*\n"
            f"Mode: `{config.EXECUTION_MODE}`\n"
            f"Pairs: `{', '.join(config.SCAN_PAIRS)}`\n"
            f"Hedge: `{'ON' if config.ENABLE_DELTA_HEDGE else 'OFF'}`\n\n"
            f"Send /help for commands."
        )

        while not stop_event.is_set():
            try:
                updates = await self._get_updates()
                for update in updates:
                    await self._handle_update(update)
            except Exception as e:
                log.warning(f"[Notifier] Polling error: {e}")

            await asyncio.sleep(3)

        # Cancel the background sender on shutdown
        if self._sender_task and not self._sender_task.done():
            self._sender_task.cancel()

        await self.send("🛑 *Engine shutting down.* Goodbye.")


    async def _get_updates(self) -> list:
        url = (
            f"https://api.telegram.org/bot{self._token}/getUpdates"
            f"?offset={self._update_offset}&timeout=2"
        )
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json()
                    updates = data.get("result", [])
                    if updates:
                        self._update_offset = updates[-1]["update_id"] + 1
                    return updates
        except Exception:
            return []

    async def _handle_update(self, update: dict) -> None:
        """Route incoming Telegram messages to command handlers."""
        message = update.get("message", {})
        text    = message.get("text", "").strip().lower()
        sender  = message.get("from", {}).get("first_name", "unknown")

        # Security: dual-check chat_id AND user id.
        # chat_id is visible in group metadata — verify the sender uid too.
        chat_id    = str(message.get("chat", {}).get("id", ""))
        sender_uid = str(message.get("from", {}).get("id", ""))

        if chat_id != str(self._chat_id):
            log.debug(f"[Notifier] Ignoring msg from unauthorized chat_id={chat_id}")
            return

        log.info(f"[Notifier] Command from {sender} (uid={sender_uid}): {text}")

        if text == "/status":
            await self._cmd_status()

        elif text == "/pause":
            self.pause(sender=sender)
            await self.send(
                f"⏸ *Engine PAUSED* by {sender}.\n"
                f"No new trades will be placed.\n"
                f"Send /resume to restart."
            )

        elif text == "/resume":
            self.resume(sender=sender)
            await self.send(f"▶️ *Engine RESUMED* by {sender}. Trading is active.")

        elif text == "/hedges":
            await self._cmd_hedges()

        elif text in ("/help", "/start"):
            await self.send(
                "*QuantSwarm Commands:*\n\n"
                "/status — live P&L + balances\n"
                "/pause  — pause all trading\n"
                "/resume — resume trading\n"
                "/hedges — show open hedge positions\n"
                "/help   — this message"
            )


    async def _cmd_status(self) -> None:
        """Send a live status report."""
        if self._risk_mgr:
            s = self._risk_mgr.status_summary()
            pnl_emoji = "📈" if s["daily_pnl"] >= 0 else "📉"
            cb_status = "🔴 TRIPPED" if s["daily_loss_breached"] else "🟢 OK"
            paused    = "⏸ YES" if self._paused else "▶️ NO"

            msg = (
                f"*QuantSwarm Status*\n\n"
                f"{pnl_emoji} Daily P&L: `${s['daily_pnl']:+.4f}` USDT\n"
                f"🔒 Circuit Breaker: `{cb_status}`\n"
                f"📊 Open Positions: `{s['total_open']}`\n"
                f"⏸ Engine Paused: `{paused}`\n"
                f"💹 Mode: `{config.EXECUTION_MODE}`"
            )
        else:
            msg = "Status unavailable (risk manager not connected)."
        await self.send(msg)


    async def _cmd_hedges(self) -> None:
        """Show all open hedge positions."""
        from hedge_manager import HedgeManager
        # Access via rebalancer reference
        if self._rebal and self._rebal._hedge:
            positions = self._rebal._hedge.get_open_positions()
            if not positions:
                await self.send("🟢 No open hedge positions.")
                return
            lines = ["*Open Hedge Positions:*\n"]
            for sym, pos in positions.items():
                lines.append(
                    f"• `{sym}`: {pos['quantity']:.4f} SHORT | "
                    f"held {pos['hours_held']:.1f}h | "
                    f"funding: `${pos['accrued_funding']:+.5f}`"
                )
            await self.send("\n".join(lines))
        else:
            await self.send("Hedge manager not connected.")

    # ─── Convenience Alert Helpers ────────────────────────────────────────────

    async def alert_trade_filled(
        self,
        symbol: str,
        realized_pnl: float,
        execution_ms: int,
        buy_ex: str,
        sell_ex: str,
    ) -> None:
        emoji = "💰" if realized_pnl >= 0 else "📉"
        await self.send(
            f"{emoji} *Trade Filled*\n"
            f"Pair: `{symbol}`\n"
            f"BUY `{buy_ex.upper()}` → SELL `{sell_ex.upper()}`\n"
            f"Realized P&L: `${realized_pnl:+.4f}` USDT\n"
            f"Latency: `{execution_ms}ms`"
        )

    async def alert_restock_complete(self, symbol: str, quantity: float, price: float) -> None:
        base = symbol.split("/")[0]
        await self.send(
            f"🔄 *Auto-Restock Complete*\n"
            f"`{symbol}` | bought `{quantity:.6f} {base}` @ `${price:.4f}`\n"
            f"Hedge SHORT opened ✅ | Ready for next arb."
        )

    async def alert_warning(self, message: str) -> None:
        await self.send(f"⚠️ *Warning*\n{message}")

    async def alert_critical(self, message: str) -> None:
        await self.send(f"🚨 *CRITICAL*\n{message}")

    async def send_hourly_summary(self, risk_manager) -> None:
        s = risk_manager.status_summary()
        pnl_emoji = "📈" if s["daily_pnl"] >= 0 else "📉"
        await self.send(
            f"{pnl_emoji} *Hourly Summary*\n"
            f"Daily P&L: `${s['daily_pnl']:+.4f}` USDT\n"
            f"Open Positions: `{s['total_open']}`\n"
            f"Circuit Breaker: `{'TRIPPED' if s['daily_loss_breached'] else 'OK'}`"
        )
