# Crypto Data Prep

Python toolkit for cryptocurrency data fetching, futures basis analysis, and backtesting. Fetches CME futures from Databento local CSV (default) or Interactive Brokers, spot prices from IBKR Crypto (BTC.USD PAXOS), and computes basis trade metrics.

## Features

- **Multi-source futures data** - Databento local CSV (default, no connection needed) or IBKR (requires TWS/Gateway)
- **Multi-source spot prices** - Coinbase, Binance, IBKR ETF proxy (IBIT/FBTC/GBTC), IBKR Crypto (BTC.USD PAXOS)
- **Config-driven pairs** - BTC, ETH (or custom) with per-pair spot/futures settings
- **Futures basis analysis** - Absolute basis, percentage, annualized basis, days to expiry
- **Continuous futures** - Auto-rolling across contract expiries (Databento front-month rolling or IBKR ContFuture)
- **Backtesting engine** - Signal-based basis trade backtester with P&L, Sharpe ratio, max drawdown
- **CSV export** - All data exportable for further analysis

## Installation

```bash
pip install -e .                    # Basic install
pip install -e ".[ibkr]"            # With IBKR support (requires TWS/IB Gateway)
pip install -e ".[dev]"             # With dev tools (pytest, black, flake8)
```

## Requirements

- Python >= 3.8
- Databento CSV files in `databento/<PAIR>/` folder (for default futures source)
- TWS or IB Gateway running for IBKR features (port 7496 for TWS Live, 7497 for Paper)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick Start

### Fetch spot + futures basis data

The front-month contract is the futures contract with the nearest upcoming expiry. Without `--expiry`, the tool auto-selects it (e.g., on Feb 15 2026 → Feb 2026 contract; once expired → Mar 2026).

```bash
# Front-month contract, date range = previous expiry to current expiry - 1
python examples/accumulate_futures.py

# Specific contract expiry
python examples/accumulate_futures.py --expiry 202603

# All 12 months of a year (MBTF4, MBTG4, ..., MBTZ4)
python examples/accumulate_futures.py --year 2024

# Use IBKR for futures instead of Databento
python examples/accumulate_futures.py --futures-source ibkr

# ETH pair (from config)
python examples/accumulate_futures.py --pair ETH

# Custom output path
python examples/accumulate_futures.py --expiry 202603 -o data/my_basis.csv

# Date range: prev expiry+1 to curr expiry (instead of default prev expiry to curr expiry-1)
python examples/accumulate_futures.py --expiry 202402 --end-on-expiry
```

### Accumulate + Backtest (one step)

Accumulates basis data then runs backtest in one command. `--holding-days` (default: 30) sets the maximum days a trade stays open before automatic exit.

```bash
python scripts/accumulate_and_backtest.py --expiry 202402
python scripts/accumulate_and_backtest.py --year 2024
python scripts/accumulate_and_backtest.py --expiry 202402 --pair ETH
python scripts/accumulate_and_backtest.py --expiry 202603 --holding-days 15
python scripts/accumulate_and_backtest.py --futures-source ibkr --holding-days 30

# Custom signal thresholds
python scripts/accumulate_and_backtest.py --expiry 202402 --entry-threshold 0.008 --exit-threshold 0.04

# Use optimized params from optimize_signals.py
python scripts/accumulate_and_backtest.py --year 2024 --params data/best_params.json
```

### Optimize signal thresholds

Grid search over entry, stop-loss, exit thresholds and holding days to find the parameter combination that maximizes total return.

```bash
# Optimize on pre-existing CSV
python scripts/optimize_signals.py --data data/BTC_futures_basis_202402.csv

# Accumulate then optimize
python scripts/optimize_signals.py --expiry 202402
python scripts/optimize_signals.py --year 2024
python scripts/optimize_signals.py --year 2024 --pair ETH

# Save best params to JSON for use with accumulate_and_backtest.py --params
python scripts/optimize_signals.py --year 2024 --save-params data/best_params.json

# Show more results
python scripts/optimize_signals.py --data data/BTC_futures_basis_202402.csv --top 30
```

### Continuous futures with auto-rolling

Fetches spot (BTC.USD on PAXOS from IBKR) and continuous futures (Databento front-month rolling or IBKR ContFuture):

```bash
python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10
python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10 --pair ETH
python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10 --futures-source ibkr
python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10 --bar-size "1 hour"
python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10 -o data/my_cont.csv --no-csv
```

### CLI commands

```bash
python main.py fetch-spot                                  # Spot prices from Coinbase/Binance
python main.py fetch-futures                               # CME futures via IBKR
python main.py fetch-historical --symbol IBIT --days 30    # Historical ETF data
python main.py backtest --data data/file.csv --holding-days 30
```

## Backtest Strategy

The backtester implements a **basis trade** strategy: long spot + short futures. It profits when the futures premium (basis) narrows toward zero at expiry, regardless of BTC price direction.

### Signal Generation

Basis is normalized to a 30-day (monthly) equivalent so signals are comparable across different days-to-expiry:

```
basis_pct     = (futures_price - spot_price) / spot_price
monthly_basis = basis_pct × (30 / days_to_expiry)
```

| Signal | Condition (default) | Action |
|--------|-----------|--------|
| STOP_LOSS | basis_pct < 0 or monthly_basis < 0.2% | Exit — basis collapsed |
| FULL_EXIT | monthly_basis > 3.5% | Exit — basis widened, cut losses |
| PARTIAL_EXIT | monthly_basis > midpoint(entry, exit) | Exit — basis elevated, reduce risk |
| STRONG_ENTRY | monthly_basis > 0.5% | Enter trade |
| NO_ENTRY | all other cases | Stay flat |

All thresholds are configurable via `--entry-threshold`, `--stop-loss-threshold`, `--exit-threshold` on `accumulate_and_backtest.py`, or swept automatically by `optimize_signals.py`.

### Profit Calculation

```
spot_pnl     = (exit_spot - entry_spot) × position_size       # long spot
futures_pnl  = (entry_futures - exit_futures) × position_size  # short futures
funding_cost = (annual_rate / 365) × holding_days × position_value
realized_pnl = spot_pnl + futures_pnl - funding_cost
```

Profit ≈ entry basis - exit basis - funding cost. The trade captures the structural tendency of futures premium to converge toward spot at expiry.

### Trade Examples

**Profitable trade (basis narrows):** Enter at STRONG_ENTRY (1.0%), hold as basis converges, exit at STOP_LOSS (<0.2%):
```
Entry (25 DTE): spot=50,000  futures=50,500  basis=$500 (1.0%)
Exit  ( 5 DTE): spot=52,000  futures=52,013  basis=$13  (0.025%)
spot_pnl=+2,000  futures_pnl=-1,513  funding=-137  → realized=+$350 (+0.7%)
```

**Losing trade (basis widens):** Enter at ACCEPTABLE_ENTRY (0.5%), basis widens, exit at FULL_EXIT (3.5%):
```
Entry (20 DTE): spot=50,000  futures=50,167  basis=$167 (0.33%)
Exit  (10 DTE): spot=51,000  futures=51,595  basis=$595 (1.17%)
spot_pnl=+1,000  futures_pnl=-1,428  funding=-68  → realized=-$496 (-1.0%)
```

FULL_EXIT and PARTIAL_EXIT are **risk management** exits — basis widening means the short futures leg is losing more than the long spot leg gains.

## Output Format

CSV output includes the following columns:

```
date,contract,spot_price,futures_price,futures_expiry,basis_absolute,basis_percent,monthly_basis,annualized_basis,days_to_expiry
```

| Column | Description |
|--------|-------------|
| `date` | Trading date |
| `contract` | CME contract name (e.g., `MBTG4` = MBT Feb 2024). Format: `<symbol><month_code><year_digit>` |
| `spot_price` | BTC spot price (IBKR BTC.USD PAXOS or Binance BTCUSDT) |
| `futures_price` | Front-month CME futures price |
| `futures_expiry` | Expiry date of the front-month contract |
| `basis_absolute` | `futures_price - spot_price` |
| `basis_percent` | `(basis_absolute / spot_price) * 100` |
| `monthly_basis` | `basis_percent * (30 / days_to_expiry)` — used by backtester for signal generation |
| `annualized_basis` | `basis_percent * (365 / days_to_expiry)` |
| `days_to_expiry` | Days remaining until contract expiry |

## Project Structure

```
crypto-data-prep/
├── src/crypto_data/
│   ├── data/
│   │   ├── base.py            # BaseFetcher ABC
│   │   ├── coinbase.py        # Coinbase spot fetcher
│   │   ├── binance.py         # Binance spot + perpetual futures
│   │   ├── ibkr.py            # IBKR fetchers (spot, futures, continuous)
│   │   ├── databento.py       # Databento local CSV fetcher
│   │   └── accumulator.py     # FuturesAccumulator (basis analysis + CSV export)
│   ├── backtest/
│   │   ├── engine.py          # Backtester with signal-based entries/exits
│   │   └── costs.py           # Transaction cost modeling
│   └── utils/
│       ├── expiry.py          # CME expiry calculations (last Friday of month)
│       ├── config.py          # ConfigLoader
│       └── logging.py         # LoggingMixin
├── scripts/
│   ├── accumulate_and_backtest.py  # Accumulate basis data + run backtest in one step
│   └── optimize_signals.py        # Grid search optimizer for signal thresholds
├── examples/
│   ├── accumulate_futures.py       # Basis data accumulation (IBKR spot + Databento/IBKR futures)
│   └── fetch_continuous_futures.py # Continuous futures (IBKR spot PAXOS + Databento/IBKR ContFuture)
├── databento/
│   └── BTC/                        # Databento OHLCV-1d CSV per pair
│       └── glbx-mdp3-*.ohlcv-1d.csv
├── tests/
│   ├── test_fetch_historical.py
│   ├── test_databento.py
│   ├── test_config_pairs.py
│   └── test_get_historical_continuous_futures.py
├── config/
│   ├── config.example.json
│   └── config.json            # (gitignored)
├── data/                      # Output CSV files
├── main.py                    # CLI entry point
└── setup.py
```

## Configuration

Copy `config/config.example.json` to `config/config.json`:

```json
{
  "ibkr": {
    "host": "127.0.0.1",
    "port": 7496,
    "client_id": 1
  },
  "databento": {
    "data_dir": "databento"
  },
  "pairs": {
    "BTC": {
      "spot": { "symbol": "BTC", "exchange": "PAXOS", "currency": "USD" },
      "futures": { "symbol": "MBT", "exchange": "CME" }
    },
    "ETH": {
      "spot": { "symbol": "ETH", "exchange": "PAXOS", "currency": "USD" },
      "futures": { "symbol": "MET", "exchange": "CME" }
    }
  },
  "default_pair": "BTC"
}
```

| Port | Description |
|------|-------------|
| 7496 | TWS Live |
| 7497 | TWS Paper Trading |
| 4001 | IB Gateway Live |
| 4002 | IB Gateway Paper |

### Databento Data

Place Databento OHLCV-1d CSV files in `databento/<PAIR>/` (e.g., `databento/BTC/`). The fetcher auto-discovers `*.ohlcv-1d.csv` files in the pair subfolder.

## Testing

```bash
pytest tests/ -v                              # Run all tests
pytest tests/test_databento.py -v             # Databento fetcher tests
pytest tests/test_fetch_historical.py -v      # Historical fetcher tests
pytest tests/ -k "test_from_config"           # Pattern matching
```

## Python API

```python
from crypto_data.data.accumulator import FuturesAccumulator
from crypto_data.utils.config import ConfigLoader
from crypto_data.utils.expiry import get_front_month_expiry_str, get_last_friday_of_month
from datetime import datetime, timedelta

config_loader = ConfigLoader("config/config.json")
acc = FuturesAccumulator.from_config(config_loader.ibkr)

pair_config = config_loader.get_pair("BTC")
spot_config = pair_config["spot"]

# Single contract basis (Databento futures, IBKR spot)
data = acc.accumulate(
    start_date=datetime(2026, 2, 1),
    end_date=datetime(2026, 2, 26),
    expiry="202602",
    symbol="MBT",
    spot_source="ibkr",
    spot_config=spot_config,
    futures_source="databento",
    databento_dir="databento/BTC",
)

# Or use IBKR for futures
data = acc.accumulate(
    start_date=datetime(2026, 2, 1),
    end_date=datetime(2026, 2, 26),
    expiry="202602",
    symbol="MBT",
    spot_source="ibkr",
    spot_config=spot_config,
    futures_source="ibkr",
)

# Export to CSV
acc.to_csv(data, "data/output.csv")
```

## FAQ

### What is grid search in `optimize_signals.py`?

Grid search is a brute-force optimization that tries every combination of parameter values from a predefined grid:

| Parameter | Range | Step | Values |
|-----------|-------|------|--------|
| entry_threshold | 0.2% - 2.0% | 0.2% | 10 |
| stop_loss_threshold | 0.1% - 0.5% | 0.1% | 5 |
| exit_threshold | 2.0% - 6.0% | 0.5% | 9 |
| holding_days | 10 - 60 | 10 | 6 |

This produces ~2,700 combinations (after filtering invalid ones where entry <= stop or exit <= entry). Each combination runs a full backtest, then results are ranked by total return. The best parameters can be saved to JSON (`--save-params`) and loaded into `accumulate_and_backtest.py` (`--params`).

**Example workflow:**

```bash
# Step 1: Optimize on 2024 data and save best params
python scripts/optimize_signals.py --year 2024 --save-params data/best_params.json

# Step 2: Run backtest on 2025 data using optimized params
python scripts/accumulate_and_backtest.py --year 2025 --params data/best_params.json
```

Sample result (2025 with params optimized on 2024):
```
Period:          2024-12-27 to 2025-12-24
Total Return:    14.74%
Sharpe Ratio:    15.29
Max Drawdown:    0.81%
Win Rate:        81.25% (26W / 6L)
Total Trades:    32
Avg Win:         1.34%
Avg Loss:        -1.03%
Profit Factor:   1.30
Initial Capital: $200,000.00
Final Capital:   $229,473.72
```

**Caveat:** Optimized parameters may overfit to historical data — the best parameters on 2024 data may not be optimal for 2025.
