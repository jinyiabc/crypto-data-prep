# Crypto Data Prep

Python toolkit for cryptocurrency data fetching, futures basis analysis, and backtesting. Fetches spot prices from Binance/Coinbase and CME futures from Interactive Brokers, then computes basis trade metrics.

## Features

- **Multi-source spot prices** - Coinbase, Binance, IBKR ETF proxy (IBIT/FBTC/GBTC)
- **CME futures data** - Single contract and continuous futures via IBKR
- **Futures basis analysis** - Absolute basis, percentage, annualized basis, days to expiry
- **Continuous futures** - Auto-rolling across contract expiries (manual + IBKR ContFuture)
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
- TWS or IB Gateway running for IBKR features (port 7496 for TWS Live, 7497 for Paper)

## Quick Start

### Fetch spot + futures basis data

```bash
python examples/accumulate_futures.py --days 30
```

### Continuous futures with auto-rolling

```bash
python examples/accumulate_futures.py --continuous --days 90
```

### Specify contract expiry and output file

```bash
python examples/accumulate_futures.py --days 60 --expiry 202603 -o data/my_basis.csv
```

### CLI commands

```bash
python main.py fetch-spot                                  # Spot prices from Coinbase/Binance
python main.py fetch-futures                               # CME futures via IBKR
python main.py fetch-historical --symbol IBIT --days 30    # Historical ETF data
python main.py backtest --data data/file.csv --holding-days 30
```

## Output Format

CSV output includes the following columns:

```
date,spot_price,futures_price,future_continuous,futures_expiry,basis_absolute,basis_percent,annualized_basis,days_to_expiry
```

| Column | Description |
|--------|-------------|
| `date` | Trading date |
| `spot_price` | BTC spot price from Binance (BTCUSDT) |
| `futures_price` | Front-month CME futures price |
| `future_continuous` | IBKR ContFuture continuous price (auto-rolled) |
| `futures_expiry` | Expiry date of the front-month contract |
| `basis_absolute` | `futures_price - spot_price` |
| `basis_percent` | `(basis_absolute / spot_price) * 100` |
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
│   │   └── accumulator.py     # FuturesAccumulator (basis analysis + CSV export)
│   ├── backtest/
│   │   ├── engine.py          # Backtester with signal-based entries/exits
│   │   └── costs.py           # Transaction cost modeling
│   └── utils/
│       ├── expiry.py          # CME expiry calculations (last Friday of month)
│       ├── config.py          # ConfigLoader
│       └── logging.py         # LoggingMixin
├── examples/
│   └── accumulate_futures.py  # CLI example for basis data accumulation
├── tests/
│   └── test_fetch_historical.py
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
  }
}
```

| Port | Description |
|------|-------------|
| 7496 | TWS Live |
| 7497 | TWS Paper Trading |
| 4001 | IB Gateway Live |
| 4002 | IB Gateway Paper |

## Testing

```bash
pytest tests/ -v                              # Run all tests
pytest tests/test_fetch_historical.py -v      # Specific test file
pytest tests/ -k "test_from_config"           # Pattern matching
```

## Python API

```python
from crypto_data.data.accumulator import FuturesAccumulator
from crypto_data.data.ibkr import IBKRHistoricalFetcher
from datetime import datetime, timedelta

fetcher = IBKRHistoricalFetcher(host="127.0.0.1", port=7496, client_id=2)
fetcher.connect()

acc = FuturesAccumulator(fetcher)

# Single contract basis
data = acc.accumulate(
    start_date=datetime.now() - timedelta(days=30),
    end_date=datetime.now(),
    expiry="202603",
    symbol="MBT",
    spot_symbol="BTCUSDT",
)

# Continuous futures (auto-rolling + ContFuture)
data = acc.accumulate_continuous(
    start_date=datetime.now() - timedelta(days=90),
    end_date=datetime.now(),
    symbol="MBT",
    spot_symbol="BTCUSDT",
)

# Export to CSV
acc.to_csv(data, "data/output.csv")

fetcher.disconnect()
```
