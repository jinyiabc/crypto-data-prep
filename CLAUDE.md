# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Crypto Data Prep is a Python toolkit for cryptocurrency data fetching and preparation. It provides unified interfaces to fetch spot/futures prices from multiple sources (Coinbase, Binance, IBKR, Databento) and includes a backtesting engine for basis trade strategies.

## Commands

```bash
# Install
pip install -e .                    # Basic install
pip install -e ".[ibkr]"            # With IBKR support

# Fetch data
python main.py fetch-spot                              # Spot prices from Coinbase/Binance
python main.py fetch-futures                           # CME futures via IBKR
python main.py fetch-historical --symbol IBIT --days 30  # Historical ETF data

# Continuous futures (Databento default, or IBKR with --futures-source ibkr)
python examples/fetch_continuous_futures.py --start 2025-01-01 --end 2025-12-31
python examples/fetch_continuous_futures.py --start 2025-01-01 --end 2025-12-31 --futures-source ibkr
python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10 --pair ETH --bar-size "1 hour"

# Accumulate basis data (Databento futures default, IBKR spot)
# Date range: previous expiry to current expiry - 1
python examples/accumulate_futures.py                                    # front-month, prev expiry to expiry-1
python examples/accumulate_futures.py --expiry 202603                    # Mar 2026: Feb expiry to Mar expiry-1
python examples/accumulate_futures.py --year 2024                        # all 12 months (MBTF4..MBTZ4)
python examples/accumulate_futures.py --futures-source ibkr              # use IBKR for futures
python examples/accumulate_futures.py --pair ETH                         # ETH pair from config
python examples/accumulate_futures.py --expiry 202402 --end-on-expiry    # prev expiry+1 to curr expiry

# Accumulate + Backtest (one step)
python scripts/accumulate_and_backtest.py --expiry 202402
python scripts/accumulate_and_backtest.py --year 2024
python scripts/accumulate_and_backtest.py --expiry 202402 --pair ETH
python scripts/accumulate_and_backtest.py --expiry 202603 --holding-days 15
python scripts/accumulate_and_backtest.py --futures-source ibkr --holding-days 30
python scripts/accumulate_and_backtest.py --expiry 202402 --end-on-expiry
python scripts/accumulate_and_backtest.py --expiry 202402 --entry-threshold 0.008 --exit-threshold 0.04
python scripts/accumulate_and_backtest.py --year 2024 --params data/best_params.json

# Optimize signal thresholds (grid search)
python scripts/optimize_signals.py --data data/BTC_futures_basis_202402.csv
python scripts/optimize_signals.py --year 2024
python scripts/optimize_signals.py --year 2024 --save-params data/best_params.json

# Backtest
python main.py backtest --data data/file.csv --holding-days 30
python main.py backtest --data data/my_basis.csv --holding-days 30

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
- `DatabentoLocalFetcher` - CME Micro Bitcoin Futures (MBT) from local Databento CSV files (no connection required)

### IBKR Connection

IBKR fetchers use `ib-insync` and require running TWS or IB Gateway. Connection settings in `config/config.json`:
```json
{"ibkr": {"host": "127.0.0.1", "port": 7496, "client_id": 1}}
```

Port reference: 7496=TWS Live, 7497=TWS Paper, 4001=Gateway Live, 4002=Gateway Paper

The fetcher uses `get_front_month_expiry_str()` from `utils/expiry.py` to automatically select the front-month CME contract. The front-month is the futures contract with the nearest upcoming expiry (e.g., if today is Feb 15 2026, front-month = Feb 2026 contract expiring last Friday of Feb; once expired, Mar 2026 becomes front-month).

### Backtesting (`src/crypto_data/backtest/`)

Standalone backtester with no external dependencies:
- `Signal` enum: STRONG_ENTRY, ACCEPTABLE_ENTRY, PARTIAL_EXIT, FULL_EXIT, STOP_LOSS, NO_ENTRY
- `Trade` dataclass: Tracks entry/exit, calculates P&L including funding costs
- `BacktestResult`: Aggregates trades, computes Sharpe ratio, max drawdown, win rate
- `Backtester.run_backtest()`: Main loop processing historical data

**Signal generation:** Basis is normalized to a 30-day (monthly) equivalent so signals are comparable regardless of days to expiry:
- `basis_pct = (futures_price - spot_price) / spot_price`
- `monthly_basis = basis_pct * (30 / days_to_expiry)`

Signal thresholds (exits checked first, all configurable via config or CLI `--entry-threshold`, `--stop-loss-threshold`, `--exit-threshold`):
- STOP_LOSS: basis_pct < 0 or monthly_basis < stop_loss_threshold (default 0.2%)
- FULL_EXIT: monthly_basis > exit_threshold (default 3.5%)
- PARTIAL_EXIT: monthly_basis > midpoint(entry, exit) (default 2.0%)
- STRONG_ENTRY: monthly_basis > entry_threshold (default 0.5%)
- NO_ENTRY: all other cases

Trades are automatically force-closed at contract boundaries (status `"contract_roll"`), preventing trades from being held across different futures contracts when data spans multiple expiries (e.g., `--year`).

**Profit logic (basis trade = long spot + short futures):**
```
spot_pnl     = (exit_spot - entry_spot) × position_size
futures_pnl  = (entry_futures - exit_futures) × position_size
funding_cost = (annual_rate / 365) × holding_days × (entry_spot × position_size)
realized_pnl = spot_pnl + futures_pnl - funding_cost
```
Profit ≈ entry basis - exit basis - funding cost. The trade profits from basis narrowing (futures premium converging toward spot), regardless of BTC direction. FULL_EXIT/PARTIAL_EXIT are risk management exits (basis widened = loss), while STOP_LOSS at <0.2% means basis converged (= profit realized).

**Trade examples:**
- Profitable: Enter at 1.0% monthly_basis (25 DTE), exit at <0.2% (5 DTE) → basis $500→$13, realized ≈ +$350 (+0.7%)
- Losing: Enter at 0.5% monthly_basis (20 DTE), exit at 3.5% (10 DTE) → basis $167→$595, realized ≈ -$496 (-1.0%)

`--holding-days` (default: 30) is the maximum number of days a trade is held open. If no exit signal is received within this period, the trade is automatically closed. Shorter values force earlier exits (more conservative); longer values give trades more time to hit a signal-based exit.

### Databento (`src/crypto_data/data/databento.py`)

`DatabentoLocalFetcher` reads pre-downloaded Databento OHLCV-1d CSV files for CME Micro Bitcoin Futures. No API key or connection required.

- Symbols: `MBT<month_code><year_digit>` (e.g., `MBTG6` = Feb 2026). CME month codes: F-Z for Jan-Dec.
- Spread symbols (containing `-`) are filtered out automatically
- Provides `get_historical_futures()` and `get_historical_continuous_futures()` with same interface as IBKR
- Data organized per pair: `databento/<PAIR>/` (e.g., `databento/BTC/`)
- Configurable base directory via `databento.data_dir` in config or `--databento-dir` CLI flag

### Scripts (`scripts/`)

- `accumulate_and_backtest.py` - Combined accumulate + backtest in one step
  - Accumulates basis data (Step 1), then runs backtest on the result (Step 2)
  - Same flags as `accumulate_futures.py` plus `--holding-days` for backtest holding period
  - `--entry-threshold`, `--stop-loss-threshold`, `--exit-threshold` - Custom signal thresholds
  - `--params PATH` - Load optimized signal params from JSON file (from `optimize_signals.py --save-params`)
  - Outputs: basis table, CSV export, trade log, backtest summary
- `optimize_signals.py` - Grid search optimizer for signal thresholds
  - Sweeps entry (0.2-2.0%), stop-loss (0.1-0.5%), exit (2.0-6.0%) thresholds and holding days (10-60)
  - `--data PATH` - Optimize on pre-existing CSV (skip accumulation)
  - `--expiry YYYYMM` / `--year YYYY` - Accumulate then optimize
  - `--save-params PATH` - Save best params to JSON file for use with `accumulate_and_backtest.py --params`
  - `--top N` - Number of top results to show (default: 20)
  - Prints ranked results table and compares against default parameters

### Examples (`examples/`)

- `accumulate_futures.py` - Basis data accumulation (IBKR spot + futures from Databento or IBKR)
  - Date range: previous expiry to current expiry - 1 (default), or prev expiry + 1 to current expiry (`--end-on-expiry`)
  - `--expiry YYYYMM` - Specific futures contract (default: front-month). Mutually exclusive with `--year`
  - `--year YYYY` - Accumulate all 12 months of a year (e.g. `--year 2024`). Mutually exclusive with `--expiry`
  - `--futures-source {databento,ibkr}` - Futures data source (default: databento)
  - `--databento-dir PATH` - Databento data directory (default: from config)
  - `--pair NAME` - Investment pair from config (e.g. BTC, ETH). Default: config's `default_pair`
  - `--symbol SYM` - Override futures symbol from pair config
  - `-o PATH` - Output CSV path
- `fetch_continuous_futures.py` - Standalone continuous futures fetcher (Databento or IBKR), date-range based
  - `--futures-source {databento,ibkr}` - Futures data source (default: databento)
  - `--databento-dir PATH` - Databento data directory (default: from config)
  - `--pair NAME` - Investment pair from config (e.g. BTC, ETH). Default: config's `default_pair`
  - `--symbol SYM` - Override futures symbol from pair config

### Utils (`src/crypto_data/utils/`)

- `expiry.py` - CME futures expiry calculations (last Friday of month)
- `config.py` - ConfigLoader with defaults and JSON file loading
- `logging.py` - LoggingMixin for consistent logging

## Configuration

Copy `config/config.example.json` to `config/config.json`. The file is gitignored.

### Pair Configuration

Investment pairs are defined in config under `pairs`. Each pair specifies spot and futures contract details:

```json
{
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

- `spot`: IBKR `Crypto(symbol, exchange, currency)` contract for spot prices
- `futures`: IBKR `Future(symbol, expiry, exchange)` / `ContFuture(symbol, exchange)` for futures
- `default_pair`: Used when `--pair` is not specified on CLI
- BTC pair is included as a built-in default even without config file

## Data Format

Historical CSV format for backtesting:
```csv
date,contract,spot_price,futures_price,futures_expiry
2024-01-01,MBTF4,42000.00,42500.00,2024-01-26
```

Contract names use CME convention: `<symbol><month_code><year_digit>` (e.g., `MBTG4` = MBT Feb 2024). Month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun, N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec.

Continuous futures CSV format (`fetch_continuous_futures.py`):
```csv
date,spot_price,futures_price,basis_absolute,basis_percent,monthly_basis,annualized_basis,days_to_expiry,futures_expiry
2025-12-03,92990.50,94536.00,1545.50,1.66,2.16,26.38,23,2025-12-26
```

Spot source: IBKR `Crypto(symbol, exchange, currency)` from pair config, with `whatToShow="MIDPOINT"`. Futures source: Databento local CSV (default) or IBKR `ContFuture(symbol, exchange)` from pair config (auto-rolling).

## FAQ

### What is grid search in `optimize_signals.py`?

Grid search tries every combination of parameter values from a predefined grid (~2,700 combos: 10 entry x 5 stop x 9 exit x 6 holding_days). Each combination runs a full backtest, results are ranked by total return. Invalid combos (entry <= stop, exit <= entry) are skipped. Best params can be saved to JSON (`--save-params`) and loaded by `accumulate_and_backtest.py --params`. Caveat: optimized parameters may overfit to historical data.
