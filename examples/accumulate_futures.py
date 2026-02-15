#!/usr/bin/env python3
"""
Example: Accumulate futures + spot data and export to CSV.

Date range: start = previous expiry date, end = current expiry date - 1.

Requirements:
    - For --futures-source databento (default): Databento CSV in databento/<PAIR>/ folder
    - For --futures-source ibkr or spot from IBKR: TWS or IB Gateway running
    - pip install -e ".[ibkr]"  (only when using IBKR)

Usage:
    python examples/accumulate_futures.py                           # front-month, full expiry month
    python examples/accumulate_futures.py --expiry 202603           # Mar 2026 contract, Feb expiry to Mar expiry-1
    python examples/accumulate_futures.py --year 2024               # all 12 months of 2024
    python examples/accumulate_futures.py --futures-source ibkr     # use IBKR for futures
    python examples/accumulate_futures.py --pair ETH                # ETH pair from config
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_data.data.accumulator import FuturesAccumulator, format_contract_name
from crypto_data.utils.config import ConfigLoader
from crypto_data.utils.expiry import get_front_month_expiry_str, get_last_friday_of_month


def get_date_range(expiry_str, end_on_expiry):
    """Compute start/end dates for a given expiry YYYYMM."""
    expiry_year = int(expiry_str[:4])
    expiry_month = int(expiry_str[4:6])

    if expiry_month == 1:
        prev_year, prev_month = expiry_year - 1, 12
    else:
        prev_year, prev_month = expiry_year, expiry_month - 1
    prev_expiry = get_last_friday_of_month(prev_year, prev_month)
    expiry_date = get_last_friday_of_month(expiry_year, expiry_month)

    if end_on_expiry:
        start_date = prev_expiry + timedelta(days=1)
        end_date = expiry_date
    else:
        start_date = prev_expiry
        end_date = expiry_date - timedelta(days=1)

    return start_date, end_date


def main():
    parser = argparse.ArgumentParser(description="Accumulate futures basis data")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--expiry", help="Futures expiry YYYYMM (default: front-month)")
    group.add_argument("--year", type=int, help="Accumulate all 12 months of a year (e.g. 2024)")
    parser.add_argument("--pair", help="Investment pair from config (e.g. BTC, ETH). Default: config's default_pair")
    parser.add_argument("--symbol", help="Override futures symbol (default: from pair config)")
    parser.add_argument("--output", "-o", help="Output CSV path")
    parser.add_argument("--futures-source", choices=["databento", "ibkr"], default="databento",
                        help="Futures data source (default: databento)")
    parser.add_argument("--databento-dir", help="Databento data directory (default: from config or 'databento')")
    parser.add_argument("--end-on-expiry", action="store_true",
                        help="Date range: prev expiry+1 to curr expiry (default: prev expiry to curr expiry-1)")
    parser.add_argument("--config", "-c", default="config/config.json", help="Config file path")
    args = parser.parse_args()

    config_loader = ConfigLoader(args.config)
    acc = FuturesAccumulator.from_config(config_loader.ibkr)

    # Resolve pair config
    pair_name = args.pair or config_loader.default_pair
    pair_config = config_loader.get_pair(pair_name)
    spot_config = pair_config["spot"]
    futures_symbol = args.symbol or pair_config["futures"]["symbol"]
    futures_exchange = pair_config["futures"].get("exchange", "CME")

    # Resolve databento dir from CLI flag or config, with pair subfolder
    databento_base = args.databento_dir or config_loader.databento.get("data_dir", "databento")
    databento_dir = str(Path(databento_base) / pair_name)

    # Only connect to IBKR when needed
    needs_ibkr = (args.futures_source == "ibkr")  # spot_source is always ibkr here
    if needs_ibkr:
        if not acc.fetcher.connect():
            print("[X] Failed to connect to IBKR. Is TWS/Gateway running?")
            sys.exit(1)

    # Build list of expiries
    if args.year:
        expiry_list = [f"{args.year}{m:02d}" for m in range(1, 13)]
    elif args.expiry:
        expiry_list = [args.expiry]
    else:
        expiry_list = [get_front_month_expiry_str()]

    # Accumulate data for each expiry
    all_data = []
    for expiry_str in expiry_list:
        start_date, end_date = get_date_range(expiry_str, args.end_on_expiry)
        contract_name = format_contract_name(futures_symbol, expiry_str)

        if len(expiry_list) > 1:
            print(f"\n--- {contract_name}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ---")

        data = acc.accumulate(
            start_date=start_date,
            end_date=end_date,
            expiry=expiry_str,
            symbol=futures_symbol,
            exchange=futures_exchange,
            spot_source="ibkr",
            spot_config=spot_config,
            futures_source=args.futures_source,
            databento_dir=databento_dir,
        )

        if data:
            all_data.extend(data)
        elif len(expiry_list) > 1:
            print(f"    [!] No data for {contract_name}, skipping")

    if needs_ibkr:
        acc.fetcher.disconnect()

    if not all_data:
        print("[X] No data returned.")
        sys.exit(1)

    # Print summary table
    print(f"\n{'Date':<12} {'Contract':<12} {'Spot':>12} {'Futures':>12} {'Basis':>10} {'Basis%':>8} {'Monthly%':>9} {'Annual%':>9} {'DTE':>5}")
    print("-" * 94)
    for row in all_data:
        print(
            f"{row['date'].strftime('%Y-%m-%d'):<12} "
            f"{row.get('contract', ''):<12} "
            f"{row['spot_price']:>12,.2f} "
            f"{row['futures_price']:>12,.2f} "
            f"{row['basis_absolute']:>10,.2f} "
            f"{row['basis_percent']:>7.2f}% "
            f"{row['monthly_basis']:>8.2f}% "
            f"{row['annualized_basis']:>8.2f}% "
            f"{row['days_to_expiry']:>5d}"
        )

    # Export to CSV
    if args.year:
        default_output = f"data/{pair_name}_futures_basis_{args.year}.csv"
    else:
        default_output = f"data/{pair_name}_futures_basis_{expiry_list[0]}.csv"
    output_file = args.output or default_output
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    acc.to_csv(all_data, output_file)
    print(f"\nSaved {len(all_data)} rows to {output_file}")


if __name__ == "__main__":
    main()
