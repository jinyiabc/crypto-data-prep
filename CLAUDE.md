# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Crypto Data Prep is a Python toolkit for cryptocurrency data fetching and preparation. It provides unified interfaces to fetch spot/futures prices from multiple sources (Coinbase, Binance, IBKR) and includes a backtesting engine for basis trade strategies.

## Commands

```bash
# Install
pip install -e .                    # Basic install
pip install -e ".[ibkr]"            # With IBKR support

# Fetch data
python main.py fetch-spot                              # Spot prices from Coinbase/Binance
python main.py fetch-futures                           # CME futures via IBKR
python main.py fetch-historical --symbol IBIT --days 30  # Historical ETF data

# Continuous futures (IBKR spot BTC.USD PAXOS + ContFuture)
python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10
python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10 --symbol BTC --bar-size "1 hour"

# Backtest
python main.py backtest --data data/file.csv --holding-days 30

# Tests
pytest tests/ -v                              # Run all tests
pytest tests/test_fetch_historical.py -v      # Run specific test file
pytest tests/ -k "test_from_config"           # Run tests matching pattern
```

## Architecture

### Data Fetchers (`src/crypto_data/data/`)

All fetchers inherit from `BaseFetcher` (ABC) which defines:
- `fetch_spot_price()` → `Optional[float]`
- `fetch_futures_price(expiry)` → `Optional[Dict]`
- `fetch_basis_data(expiry)` → Combined spot + futures + basis calculations

**Implementations:**
- `CoinbaseFetcher` - BTC-USD spot via Coinbase public API
- `BinanceFetcher` - BTCUSDT spot + perpetual futures via Binance API
- `IBKRFetcher` - CME futures + ETF-derived spot via Interactive Brokers (requires TWS/IB Gateway)
- `IBKRHistoricalFetcher` - Historical data from IBKR (spot via Crypto BTC.USD PAXOS, futures via ContFuture)

### IBKR Connection

IBKR fetchers use `ib-insync` and require running TWS or IB Gateway. Connection settings in `config/config.json`:
```json
{"ibkr": {"host": "127.0.0.1", "port": 7496, "client_id": 1}}
```

Port reference: 7496=TWS Live, 7497=TWS Paper, 4001=Gateway Live, 4002=Gateway Paper

The fetcher uses `get_front_month_expiry_str()` from `utils/expiry.py` to automatically select the front-month CME contract.

### Backtesting (`src/crypto_data/backtest/`)

Standalone backtester with no external dependencies:
- `Signal` enum: STRONG_ENTRY, ACCEPTABLE_ENTRY, PARTIAL_EXIT, FULL_EXIT, STOP_LOSS, NO_ENTRY
- `Trade` dataclass: Tracks entry/exit, calculates P&L including funding costs
- `BacktestResult`: Aggregates trades, computes Sharpe ratio, max drawdown, win rate
- `Backtester.run_backtest()`: Main loop processing historical data

Signal thresholds based on monthly basis:
- \>1.0% → STRONG_ENTRY
- 0.5-1.0% → ACCEPTABLE_ENTRY
- \>2.5% → PARTIAL_EXIT
- \>3.5% → FULL_EXIT
- <0.2% or negative → STOP_LOSS

### Examples (`examples/`)

- `accumulate_futures.py` - Basis data accumulation (Binance spot + IBKR futures), supports `--continuous` for ContFuture
- `fetch_continuous_futures.py` - Standalone continuous futures fetcher with IBKR spot (`Crypto('BTC', 'PAXOS', 'USD')`) + IBKR ContFuture, date-range based (`--start`/`--end`)

### Utils (`src/crypto_data/utils/`)

- `expiry.py` - CME futures expiry calculations (last Friday of month)
- `config.py` - ConfigLoader with defaults and JSON file loading
- `logging.py` - LoggingMixin for consistent logging

## Configuration

Copy `config/config.example.json` to `config/config.json`. The file is gitignored.

## Data Format

Historical CSV format for backtesting:
```csv
date,spot_price,futures_price,futures_expiry
2024-01-01,42000.00,42500.00,2024-01-26
```

Continuous futures CSV format (`fetch_continuous_futures.py`):
```csv
date,spot_price,futures_price,basis_absolute,basis_percent,annualized_basis,days_to_expiry,futures_expiry
2025-12-03,92990.50,94536.00,1545.50,1.66,26.38,23,2025-12-26
```

Spot source: IBKR `Crypto('BTC', 'PAXOS', 'USD')` with `whatToShow="MIDPOINT"`. Futures source: IBKR `ContFuture` (auto-rolling).
