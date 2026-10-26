-- ============================================================
-- quant_swarm_db Schema — v2 (Production)
-- Run this to initialize or upgrade your database.
-- ============================================================

CREATE DATABASE IF NOT EXISTS quant_swarm_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE quant_swarm_db;

-- ─── Arbitrage Trade Ledger ──────────────────────────────────
-- Every trade attempt is logged here, including paper trades.
CREATE TABLE IF NOT EXISTS arbitrage_ledger (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    timestamp           TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),  -- millisecond precision
    symbol              VARCHAR(20)  NOT NULL,
    exchange_buy        VARCHAR(50)  NOT NULL,
    exchange_sell       VARCHAR(50)  NOT NULL,

    -- Requested / estimated values (from quant node)
    buy_price           DECIMAL(18, 8) NOT NULL DEFAULT 0,
    sell_price          DECIMAL(18, 8) NOT NULL DEFAULT 0,
    net_profit_usdt     DECIMAL(18, 8) NOT NULL DEFAULT 0,  -- estimated

    -- Actual fill values (from exchange response)
    executed_qty        DECIMAL(18, 8) NOT NULL DEFAULT 0,
    actual_buy_price    DECIMAL(18, 8) NOT NULL DEFAULT 0,
    actual_sell_price   DECIMAL(18, 8) NOT NULL DEFAULT 0,
    realized_pnl        DECIMAL(18, 8) NOT NULL DEFAULT 0,  -- actual

    -- Execution metadata
    execution_ms        INT           NOT NULL DEFAULT 0,   -- dual-leg wall-clock latency
    execution_status    VARCHAR(20)   NOT NULL,             -- FILLED | PARTIAL | FAILED | PAPER

    INDEX idx_symbol    (symbol),
    INDEX idx_timestamp (timestamp),
    INDEX idx_status    (execution_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ─── Balance Snapshots ────────────────────────────────────────
-- Historical record of exchange balances from the monitor.
-- Used to track fund allocation and rebalancing needs.
CREATE TABLE IF NOT EXISTS balance_snapshots (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    timestamp   TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    exchange    VARCHAR(50)  NOT NULL,
    asset       VARCHAR(20)  NOT NULL,
    free        DECIMAL(18, 8) NOT NULL,
    locked      DECIMAL(18, 8) NOT NULL DEFAULT 0,

    INDEX idx_exchange_ts (exchange, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ─── Risk Events ────────────────────────────────────────────
-- Logs circuit breaker activations, balance warnings, etc.
CREATE TABLE IF NOT EXISTS risk_events (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    timestamp   TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    event_type  VARCHAR(50)  NOT NULL,   -- e.g. CIRCUIT_BREAKER, LOW_BALANCE, IMBALANCE
    severity    VARCHAR(20)  NOT NULL,   -- INFO | WARNING | CRITICAL
    symbol      VARCHAR(20),
    details     TEXT,

    INDEX idx_event_type (event_type),
    INDEX idx_timestamp  (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ─── Hedge Positions ─────────────────────────────────────────────────
-- Lifecycle of every delta-neutral perpetual hedge position opened.
-- Positive funding_collected = we received funding (bullish market).
-- Negative = we paid (bearish market).
CREATE TABLE IF NOT EXISTS hedge_positions (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    open_time           TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    close_time          TIMESTAMP(3) NULL,
    symbol              VARCHAR(20)  NOT NULL,
    perp_symbol         VARCHAR(30)  NOT NULL,    -- e.g. ETH/USDT:USDT
    quantity            DECIMAL(18, 8) NOT NULL,
    entry_price         DECIMAL(18, 8) NOT NULL,
    exit_price          DECIMAL(18, 8) NULL,
    hours_held          DECIMAL(8, 2)  NULL,
    funding_collected   DECIMAL(18, 8) NOT NULL DEFAULT 0,  -- + received, - paid
    status              VARCHAR(20)  NOT NULL,   -- OPEN | CLOSED | FAILED
    notes               TEXT,

    INDEX idx_symbol    (symbol),
    INDEX idx_open_time (open_time),
    INDEX idx_status    (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
