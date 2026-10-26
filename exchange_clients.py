"""
exchange_clients.py — CCXT Pro exchange factory.

Creates and manages authenticated exchange client instances for Binance and Bybit.
In PAPER mode, unauthenticated clients are returned (still useful for WebSocket feeds).
In LIVE mode, API credentials are injected and validated at construction.
"""

import asyncio
from typing import Dict, Optional
import ccxt.pro as ccxtpro

import config
from logger import get_logger

log = get_logger(__name__)


# ─── Exchange Registry ────────────────────────────────────────────────────────

# Holds singleton exchange instances shared across the engine
_EXCHANGES: Dict[str, ccxtpro.Exchange] = {}


def _build_exchange_config(exchange_name: str) -> dict:
    """
    Build the CCXT constructor config dict for a given exchange.
    Injects API keys if in LIVE mode; uses minimal config in PAPER mode.
    """
    keys = config.API_KEYS.get(exchange_name, {})
    base_cfg = {
        "enableRateLimit": True,       # Respect exchange rate limits
        "newUpdates":       True,       # WebSocket only sends deltas (efficient)
        "options": {
            "defaultType": "spot",     # Ensure spot market, not futures/perp
        },
    }

    # Binance is geo-blocked in India on api.binance.com.
    # api1.binance.com is an alternate hostname that bypasses the block.
    # See: https://binance-docs.github.io/apidocs/spot/en/#general-info
    if exchange_name == "binance":
        base_cfg["hostname"] = "api1.binance.com"

    if config.EXECUTION_MODE == "LIVE" and keys.get("key"):
        base_cfg["apiKey"] = keys["key"]
        base_cfg["secret"] = keys["secret"]
        log.info(f"[{exchange_name.upper()}] Authenticated client created (LIVE mode)")
    else:
        log.info(f"[{exchange_name.upper()}] Unauthenticated client (PAPER/no keys)")

    return base_cfg


def create_exchange(name: str) -> ccxtpro.Exchange:
    """
    Instantiate a CCXT Pro exchange client by name.
    Supported: 'binance', 'bybit'
    """
    supported = {"binance": ccxtpro.binance, "bybit": ccxtpro.bybit}
    if name not in supported:
        raise ValueError(f"Unsupported exchange: '{name}'. Supported: {list(supported.keys())}")

    cfg = _build_exchange_config(name)
    exchange = supported[name](cfg)
    _EXCHANGES[name] = exchange
    return exchange


def get_exchange(name: str) -> Optional[ccxtpro.Exchange]:
    """Returns a previously created exchange instance, or None."""
    return _EXCHANGES.get(name)


async def load_markets_all() -> None:
    """
    Pre-load market metadata on all registered exchanges.
    This caches precision rules, min order sizes, etc. — must be called before trading.
    """
    tasks = []
    for name, exchange in _EXCHANGES.items():
        tasks.append(_safe_load_markets(name, exchange))
    await asyncio.gather(*tasks)


async def _safe_load_markets(name: str, exchange: ccxtpro.Exchange) -> None:
    try:
        await exchange.load_markets()
        log.info(f"[{name.upper()}] Market metadata loaded ({len(exchange.markets)} markets)")
    except Exception as e:
        log.error(f"[{name.upper()}] Failed to load markets: {e}")


async def close_all() -> None:
    """Gracefully close all WebSocket connections."""
    for name, exchange in _EXCHANGES.items():
        try:
            await exchange.close()
            log.info(f"[{name.upper()}] WebSocket connection closed")
        except Exception as e:
            log.warning(f"[{name.upper()}] Error during close: {e}")
