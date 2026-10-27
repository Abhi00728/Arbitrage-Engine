# ⚡ Arbitrage-Engine (`arbitrage_swarm/v2`)

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Asyncio](https://img.shields.io/badge/Asyncio-Event_Driven-009688?style=for-the-badge)
![SQL](https://img.shields.io/badge/SQL-Telemetry_%26_Audit-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)
![Binance](https://img.shields.io/badge/Binance-API-FCD535?style=for-the-badge&logo=binance&logoColor=black)
![Bybit](https://img.shields.io/badge/Bybit-API-f7a600?style=for-the-badge)

An institutional-grade, asynchronous cross-exchange cryptocurrency arbitrage and automated execution engine. Built with a modular microservice-style architecture in Python, the system ingests real-time multi-exchange order book feeds, evaluates fee-adjusted quantitative spreads, enforces pre-trade risk constraints, executes concurrent multi-leg orders, and actively neutralizes unhedged leg exposure.

---

## 🎯 System Overview

Capturing spatial arbitrage across fragmented cryptocurrency exchanges requires far more than simple price comparison. In live market conditions, raw spreads are rapidly eroded by taker fees, order book slippage, asymmetric network latency, and inventory depletion on one side of the trade.

**Arbitrage-Engine (`v2`)** addresses these microstructure challenges through a decoupled, fault-tolerant pipeline:
1. **Real-Time Market Ingestion:** Non-blocking WebSocket streams maintain synchronized, low-latency local order book states.
2. **Quantitative Signal Generation:** Evaluates net expected value ($+EV$) after accounting for exchange-specific fee tiers, order book depth, and execution latency penalties.
3. **Pre-Trade Risk & Inventory Gating:** Verifies margin/spot balances, maximum drawdown thresholds, and directional exposure limits prior to order dispatch.
4. **Concurrent Multi-Leg Execution & Emergency Hedging:** Dispatches simultaneous orders across venues while actively monitoring for partial fills or single-leg failures, triggering automated Delta-neutral hedging via `hedge_manager.py` within milliseconds if a counter-leg rejects.
5. **Automated Capital Rebalancing:** Tracks cross-exchange wallet skew and automates inventory rebalancing (`rebalancer.py`) to prevent capital lockup on a single venue.

---

## 🏗️ System Architecture & Data Flow

```text
                       +-----------------------+
                       |    market_feed.py     |
                       | (Async WS Ingestors)  |
                       +-----------+-----------+
                                   |
                                   v
                       +-----------------------+
                       |    quant_engine.py    |<----+
                       | (Spread, Depth & EV)  |     |
                       +-----------+-----------+     |
                                   |                 |
         +-------------------------+                 |
         |                                           |
         v                                           |
+------------------+      +--------------------+     |
| risk_manager.py  |<---->| balance_monitor.py |     |
| (Pre-Trade Gate) |      | (Wallet & Margin)  |     |
+--------+---------+      +---------+----------+     |
         |                          |                |
         | (Approved Signal)        | (Skew Alert)   |
         v                          v                |
+------------------+      +--------------------+     |
|order_executor.py |      |   rebalancer.py    |     |
|(Multi-Leg Router)|      | (Inventory Sync)   |     |
+--------+---------+      +--------------------+     |
         |                                           |
         +-----------------+-------------------------+
         | (Fill / Reject) | (Latency Telemetry)
         v                 v
+------------------+      +--------------------+
| hedge_manager.py |      |  latency_test.py   |
| (Leg-Risk/Unwind)|      | (RTT Benchmarking) |
+--------+---------+      +--------------------+
         |
         v
+----------------------------------------------+
|     logger.py  |  notifier.py  |  script.sql |
|    (Audit Logs, Real-Time Alerts & DB I/O)   |
+----------------------------------------------+
```

---

## 📂 Codebase Architecture

Every module in the repository is isolated by responsibility to prevent I/O blocking along the critical execution path:

| File / Directory | Size | Core Responsibility |
| :--- | :---: | :--- |
| **`Main.py`** | `18.6 KB` | Central orchestrator. Initializes the `asyncio` event loop, manages task lifecycles, binds state across modules, and handles graceful shutdown routines. |
| **`order_executor.py`** | `20.8 KB` | High-concurrency execution router. Dispatches simultaneous multi-leg orders (Limit/IOC/Market), manages order state machines, tracks partial fills, and handles API rate-limit backoff. |
| **`quant_engine.py`** | `19.6 KB` | Core mathematical engine. Computes real-time net spreads, walks order book depth to estimate Volume-Weighted Average Price (VWAP) slippage, and generates executable arbitrage signals. |
| **`hedge_manager.py`** | `15.2 KB` | Asymmetric leg-risk mitigation. Monitors broken or one-sided multi-leg executions and immediately triggers emergency unwinds or counter-hedges to maintain delta neutrality. |
| **`latency_test.py`** | `14.3 KB` | Network telemetry & benchmarking suite. Profiles REST and WebSocket Round-Trip Time (RTT), clock drift, and jitter across target exchange endpoints. |
| **`notifier.py`** | `13.3 KB` | Real-time telemetry & alerting daemon. Pushes asynchronous trade execution summaries, PnL updates, and critical risk/exception alerts to external webhooks. |
| **`balance_monitor.py`** | `10.3 KB` | Real-time portfolio daemon. Polls and caches spot/margin wallet balances across connected exchanges to prevent insufficient-margin order rejections. |
| **`risk_manager.py`** | `9.7 KB` | Pre-trade and post-trade risk gatekeeper. Enforces maximum position sizing, daily drawdown circuit breakers, and volatility lockouts. |
| **`rebalancer.py`** | `8.0 KB` | Automated inventory management. Detects cross-exchange capital depletion after directional arbitrage streaks and coordinates asset rebalancing. |
| **`config.py`** | `7.5 KB` | Centralized configuration management. Stores fee structures, minimum spread thresholds, risk limits, target trading pairs, and environment credentials. |
| **`logger.py`** | `5.8 KB` | Structured, non-blocking logging pipeline. Manages rotating file logs (`logs/`) for post-trade forensics, execution latency analysis, and exception tracking. |
| **`market_feed.py`** | `5.5 KB` | Asynchronous WebSocket market data consumer. Subscribes to L1/L2 order book streams with automated heartbeat monitoring and exponential-backoff reconnection. |
| **`script.sql`** | `4.4 KB` | Relational database schema. Defines tables and indices for persisting historical trade executions, order book snapshots, PnL ledgers, and latency metrics. |
| **`exchange_clients.py`** | `3.6 KB` | Unified exchange abstraction wrapper. Standardizes cryptographic request signing, session pooling, and endpoint routing across exchange APIs. |

---

## 🧮 Quantitative & Risk Mechanics

### 1. Net Profitable Spread Condition (`quant_engine.py`)
A raw price discrepancy between Exchange $A$ (Buy Leg) and Exchange $B$ (Sell Leg) is only actionable if the spread exceeds cumulative taker/maker fees, estimated depth slippage, and a safety buffer for network latency:

$$\text{Spread}_{\text{net}}(Q) = \frac{\text{VWAP}_{\text{bid}, B}(Q) \cdot (1 - f_{B}) - \text{VWAP}_{\text{ask}, A}(Q) \cdot (1 + f_{A})}{\text{VWAP}_{\text{ask}, A}(Q)} > \theta_{\text{min}} + \lambda_{\text{latency}}$$

Where:
* $Q$: Target order quantity.
* $\text{VWAP}_{\text{ask}, A}(Q), \text{VWAP}_{\text{bid}, B}(Q)$: Volume-weighted execution prices across order book levels for quantity $Q$.
* $f_A, f_B$: Exchange-specific trading fee tiers.
* $\theta_{\text{min}}$: Minimum required net return threshold (configured in `config.py`).
* $\lambda_{\text{latency}}$: Dynamic slippage penalty scaled by current RTT measurements from `latency_test.py`.

### 2. Broken-Leg Recovery (`hedge_manager.py`)
In cross-exchange arbitrage, the primary source of negative PnL is **leg risk**—where Leg A fills, but Leg B fails due to sudden order book depletion or network jitter. `hedge_manager.py` intercepts partial or failed state transitions from `order_executor.py` and immediately executes an emergency market flatten or cross-pair hedge to cap directional exposure within strict millisecond tolerances.

---

## 🚀 Getting Started

### 1. Prerequisites
* **Python:** `3.10+`
* **Database:** PostgreSQL / MySQL / SQLite (for executing `script.sql`)
* **Exchange Credentials:** API Keys & Secrets with Spot/Derivatives reading and trading permissions enabled.

### 2. Installation
```bash
# Clone the repository
git clone [https://github.com/Abhi00728/Arbitrage-Engine.git](https://github.com/Abhi00728/Arbitrage-Engine.git)
cd Arbitrage-Engine

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required dependencies
pip install aiohttp websockets asyncio cryptography psycopg2-binary python-dotenv
```

### 3. Database Initialization
Initialize the persistence layer using the provided SQL schema:
```bash
psql -U <username> -d <database_name> -f script.sql
```

### 4. Configuration
Set your API credentials and risk parameters via environment variables or inside `config.py`:
```env
EXCHANGE_A_API_KEY="your_api_key"
EXCHANGE_A_SECRET="your_api_secret"
EXCHANGE_B_API_KEY="your_api_key"
EXCHANGE_B_SECRET="your_api_secret"
DB_CONNECTION_STRING="postgresql://user:pass@localhost:5432/arbitrage_db"
NOTIFY_WEBHOOK_URL="https://your-webhook-url"
PAPER_TRADING_MODE=True
```

### 5. Running the Engine
1. **Run Network & Latency Benchmarks First:**
   ```bash
   python3 latency_test.py
   ```
2. **Launch the Core Arbitrage Swarm:**
   ```bash
   python3 Main.py
   ```

---

## ⚠️ Engineering Constraints & Hardware Considerations

While the software architecture is fully asynchronous and modularized for production resilience, live arbitrage capture is heavily bound by physical infrastructure:
* **Geographical Network Topology:** Executing from non-colocated environments (e.g., local workstations or distant cloud regions) introduces asymmetric Round-Trip Time (RTT) to exchange matching engines (primarily hosted in AWS `ap-northeast-1` Tokyo and `ap-southeast-1` Singapore). `latency_test.py` is specifically included to quantify this bottleneck before deploying live capital.
* **Runtime & OS Scheduling Jitter:** Local consumer hardware constraints, OS context switching, and Python garbage collection cycles can introduce microsecond-to-millisecond variance during high-volatility tick bursts.

---

## 📜 Disclaimer
*This repository is built for quantitative systems research and algorithmic engineering demonstration. Automated trading across cryptocurrency venues involves substantial financial, network, and smart-contract/exchange risk.
The Project is still subject to few changes, but almost ready for production*
