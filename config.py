"""
config.py — Centralized configuration manager.

Loads all settings from environment variables (.env file).
Validates required keys at startup and raises clearly if anything is missing.
All other modules import from here — single source of truth.
"""

import os
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()


def _require_env(key: str) -> str:
    """Fetch a required env var, raise ValueError if missing."""
    val = os.getenv(key)
    if not val:
        raise ValueError(
            f"[Config] Missing required environment variable: '{key}'. "
            f"Check your .env file."
        )
    return val


def _optional_env(key: str, default: str) -> str:
    return os.getenv(key, default)


# ─── Trading Parameters ─────────────────────────────────────────────────────

SCAN_PAIRS: List[str] = [
    p.strip() for p in _optional_env("SCAN_PAIRS", "ETH/USDT,SOL/USDT,LINK/USDT,BNB/USDT").split(",")
]

# Taker fee rates per exchange (override via env for VIP tiers)
FEE_TIERS: Dict[str, float] = {
    "binance": float(_optional_env("BINANCE_TAKER_FEE", "0.0004")),
    "bybit":   float(_optional_env("BYBIT_TAKER_FEE",   "0.0006")),
}

# Execution mode: PAPER | LIVE
EXECUTION_MODE: str = _optional_env("EXECUTION_MODE", "PAPER").upper()

# Maximum USDT value to commit per trade leg (each side)
MAX_ORDER_USDT: float = float(_optional_env("MAX_ORDER_USDT", "100.0"))

# Minimum net profit in USDT required before executing a trade
MIN_NET_PROFIT_USDT: float = float(_optional_env("MIN_NET_PROFIT_USDT", "0.20"))

# Daily loss limit in USDT — engine pauses trading if breached
MAX_DAILY_LOSS_USDT: float = float(_optional_env("MAX_DAILY_LOSS_USDT", "50.0"))

# Minimum free balance to keep on each side (safety buffer)
MIN_BALANCE_BUFFER_USDT: float = float(_optional_env("MIN_BALANCE_BUFFER_USDT", "20.0"))

# Reject orderbook data older than this many milliseconds (stale data filter)
MAX_DATA_AGE_MS: int = int(_optional_env("MAX_DATA_AGE_MS", "800"))

# Max concurrent open arbitrage positions across all symbols
MAX_CONCURRENT_POSITIONS: int = int(_optional_env("MAX_CONCURRENT_POSITIONS", "2"))

# How many times to retry a failed order before giving up
ORDER_RETRY_ATTEMPTS: int = int(_optional_env("ORDER_RETRY_ATTEMPTS", "2"))

# ─── Delta-Neutral Hedge ──────────────────────────────────────────────

# Enable/disable perpetual futures hedge to eliminate pre-staged inventory risk
ENABLE_DELTA_HEDGE: bool = _optional_env("ENABLE_DELTA_HEDGE", "true").lower() == "true"

# Leverage for the perp hedge position.
# 1x = safest. No liquidation risk from price moves (margin = notional).
# NEVER go above 2x for a delta-neutral hedge at small budgets.
HEDGE_LEVERAGE: float = float(_optional_env("HEDGE_LEVERAGE", "1.0"))

# Maximum funding rate (per 8h period) at which we still open a hedge.
# If rate is MORE NEGATIVE than this, shorts are paying longs heavily — skip hedging.
# Default: -0.05% per 8h. At this rate, daily hedge cost = -0.15% of position.
# On a $30 position that's $0.045/day — a small but real cost.
MAX_FUNDING_RATE_THRESHOLD: float = float(_optional_env("MAX_FUNDING_RATE_THRESHOLD", "-0.0005"))

# ─── Telegram Notifications ──────────────────────────────────────────────

# Telegram bot token from @BotFather.
# If empty, phone alerts are silently disabled (no crash).
TELEGRAM_BOT_TOKEN: str = _optional_env("TELEGRAM_BOT_TOKEN", "")

# Your personal Telegram chat_id (get from @userinfobot after messaging your bot).
TELEGRAM_CHAT_ID: str = _optional_env("TELEGRAM_CHAT_ID", "")

# ─── API Credentials ─────────────────────────────────────────────────────────

def _load_api_keys() -> Dict:
    """
    Load exchange API keys.
    In PAPER mode, keys are optional (unauthenticated clients are used).
    In LIVE mode, all keys are required.
    """
    if EXECUTION_MODE == "PAPER":
        return {
            "binance": {
                "key":    os.getenv("BINANCE_API_KEY", ""),
                "secret": os.getenv("BINANCE_SECRET", ""),
            },
            "bybit": {
                "key":    os.getenv("BYBIT_API_KEY", ""),
                "secret": os.getenv("BYBIT_SECRET", ""),
            },
        }
    else:
        return {
            "binance": {
                "key":    _require_env("BINANCE_API_KEY"),
                "secret": _require_env("BINANCE_SECRET"),
            },
            "bybit": {
                "key":    _require_env("BYBIT_API_KEY"),
                "secret": _require_env("BYBIT_SECRET"),
            },
        }


API_KEYS: Dict = _load_api_keys()

# ─── Database ────────────────────────────────────────────────────────────────

DB_CONFIG: Dict = {
    "pool_name":  "swarm_pool",
    "pool_size":  int(_optional_env("DB_POOL_SIZE", "5")),
    "host":       _optional_env("DB_HOST", "localhost"),
    "port":       int(_optional_env("DB_PORT", "3306")),
    "user":       _optional_env("DB_USER", "root"),
    "password":   _optional_env("DB_PASSWORD", ""),
    "database":   _optional_env("DB_NAME", "quant_swarm_db"),
}

# ─── Logging ─────────────────────────────────────────────────────────────────

LOG_DIR:   str = _optional_env("LOG_DIR", "logs")
LOG_LEVEL: str = _optional_env("LOG_LEVEL", "INFO").upper()

# ─── Validation Summary ──────────────────────────────────────────────────────

def print_config_summary() -> None:
    """Print a non-sensitive configuration summary at startup."""
    print("=" * 60)
    print("  ⚙️  ARBITRAGE ENGINE — CONFIGURATION LOADED")
    print("=" * 60)
    print(f"  Mode          : {EXECUTION_MODE}")
    print(f"  Pairs         : {', '.join(SCAN_PAIRS)}")
    print(f"  Max/Trade     : ${MAX_ORDER_USDT:.0f} USDT per leg")
    print(f"  Min Profit    : ${MIN_NET_PROFIT_USDT:.4f} USDT")
    print(f"  Daily Loss Cap: ${MAX_DAILY_LOSS_USDT:.0f} USDT")
    print(f"  Balance Buffer: ${MIN_BALANCE_BUFFER_USDT:.0f} USDT")
    print(f"  Data TTL      : {MAX_DATA_AGE_MS}ms")
    print(f"  Binance Fee   : {FEE_TIERS['binance']*100:.4f}%")
    print(f"  Bybit Fee     : {FEE_TIERS['bybit']*100:.4f}%")
    print(f"  Delta Hedge   : {'✅ ENABLED  (Bybit USDT-M perp)' if ENABLE_DELTA_HEDGE else '⚠️ DISABLED (inventory risk exposed)'}")
    if ENABLE_DELTA_HEDGE:
        print(f"  Hedge Leverage: {HEDGE_LEVERAGE}x")
        print(f"  Max Fund Rate : {MAX_FUNDING_RATE_THRESHOLD*100:.4f}%/8h")
    print(f"  Binance Key   : {'SET ✅' if API_KEYS['binance']['key'] else 'NOT SET ⚠️'}")
    print(f"  Bybit Key     : {'SET ✅' if API_KEYS['bybit']['key'] else 'NOT SET ⚠️'}")
    tg = 'SET ✅' if TELEGRAM_BOT_TOKEN else 'NOT SET (alerts disabled)'
    print(f"  Telegram Bot  : {tg}")
    print("=" * 60)
