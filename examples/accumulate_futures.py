#!/usr/bin/env python3
"""
Example: Accumulate futures + spot data from IBKR and export to CSV.

Requirements:
    - TWS or IB Gateway running (default port 7496)
    - pip install -e ".[ibkr]"

Usage:
    python examples/accumulate_futures.py
    python examples/accumulate_futures.py --days 60 --expiry 202603
    python examples/accumulate_futures.py --days 30 --spot-symbol BTCUSDT --output data/my_basis.csv
    python examples/accumulate_futures.py --continuous --days 90
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_data.data.accumulator import FuturesAccumulator
from crypto_data.utils.config import ConfigLoader


def main():
    parser = argparse.ArgumentParser(description="Accumulate futures basis data from IBKR")
    parser.add_argument("--days", type=int, default=30, help="Days of history (default: 30)")
    parser.add_argument("--expiry", help="Futures expiry YYYYMM (default: front-month)")
    parser.add_argument("--continuous", action="store_true", help="Use continuous futures (auto-roll across contracts)")
    parser.add_argument("--symbol", default="MBT", choices=["MBT", "BTC"], help="Futures symbol (default: MBT)")
    parser.add_argument("--spot-symbol", default="BTCUSDT", help="Binance spot pair (default: BTCUSDT)")
    parser.add_argument("--output", "-o", help="Output CSV path")
    parser.add_argument("--config", "-c", default="config/config.json", help="Config file path")
    args = parser.parse_args()

    config_loader = ConfigLoader(args.config)
    acc = FuturesAccumulator.from_config(config_loader.ibkr)

    if not acc.fetcher.connect():
        print("[X] Failed to connect to IBKR. Is TWS/Gateway running?")
        sys.exit(1)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=args.days)

    if args.continuous:
        data = acc.accumulate_continuous(
            start_date=start_date,
            end_date=end_date,
            symbol=args.symbol,
            spot_symbol=args.spot_symbol,
        )
    else:
        data = acc.accumulate(
            start_date=start_date,
            end_date=end_date,
            expiry=args.expiry,
            symbol=args.symbol,
            spot_symbol=args.spot_symbol,
        )

    acc.fetcher.disconnect()

    if not data:
        print("[X] No data returned.")
        sys.exit(1)

    # Print summary table
    has_continuous = any(row.get("future_continuous") is not None for row in data)
    if has_continuous:
        print(f"\n{'Date':<12} {'Spot':>12} {'Futures':>12} {'Continuous':>12} {'Basis':>10} {'Basis%':>8} {'Annual%':>9} {'DTE':>5}")
        print("-" * 85)
        for row in data:
            cont = row.get("future_continuous")
            cont_str = f"{cont:>12,.2f}" if cont is not None else f"{'N/A':>12}"
            print(
                f"{row['date'].strftime('%Y-%m-%d'):<12} "
                f"{row['spot_price']:>12,.2f} "
                f"{row['futures_price']:>12,.2f} "
                f"{cont_str} "
                f"{row['basis_absolute']:>10,.2f} "
                f"{row['basis_percent']:>7.2f}% "
                f"{row['annualized_basis']:>8.2f}% "
                f"{row['days_to_expiry']:>5d}"
            )
    else:
        print(f"\n{'Date':<12} {'Spot':>12} {'Futures':>12} {'Basis':>10} {'Basis%':>8} {'Annual%':>9} {'DTE':>5}")
        print("-" * 72)
        for row in data:
            print(
                f"{row['date'].strftime('%Y-%m-%d'):<12} "
                f"{row['spot_price']:>12,.2f} "
                f"{row['futures_price']:>12,.2f} "
                f"{row['basis_absolute']:>10,.2f} "
                f"{row['basis_percent']:>7.2f}% "
                f"{row['annualized_basis']:>8.2f}% "
                f"{row['days_to_expiry']:>5d}"
            )

    # Export to CSV
    mode = "continuous" if args.continuous else "basis"
    output_file = args.output or f"data/BTC_futures_{mode}_{args.days}d.csv"
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    acc.to_csv(data, output_file)
    print(f"\nSaved {len(data)} rows to {output_file}")


if __name__ == "__main__":
    main()
