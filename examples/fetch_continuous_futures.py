#!/usr/bin/env python3
"""
Fetch historical continuous futures data between start and end date.

Supports Databento local CSV (default) or IBKR ContFuture for futures prices,
and IBKR Crypto (BTC.USD on PAXOS) for spot prices.
Computes basis metrics and exports to CSV.

Requirements:
    - For --futures-source databento (default): Databento CSV in databento/<PAIR>/ folder
    - For --futures-source ibkr: TWS or IB Gateway running
    - pip install -e ".[ibkr]"  (only when using IBKR for spot or futures)

Usage:
    python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10
    python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10 --symbol BTC
    python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10 --futures-source ibkr
    python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10 --bar-size "1 hour"
    python examples/fetch_continuous_futures.py --start 2025-12-01 --end 2026-02-10 -o data/my_cont.csv
"""

import argparse
import csv
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_data.data.ibkr import IBKRHistoricalFetcher
from crypto_data.data.databento import DatabentoLocalFetcher
from crypto_data.utils.config import ConfigLoader
from crypto_data.utils.expiry import (
    generate_expiry_schedule,
    get_front_month_expiry,
)


CSV_FIELDNAMES = [
    "date",
    "spot_price",
    "futures_price",
    "basis_absolute",
    "basis_percent",
    "annualized_basis",
    "days_to_expiry",
    "futures_expiry",
]


def fetch_ibkr_spot_history(fetcher, start_date, end_date, bar_size="1 day", spot_config=None):
    """
    Fetch historical spot prices from IBKR using Crypto contract.

    Automatically chunks requests > 365 days into yearly segments.

    Args:
        fetcher: Connected IBKRHistoricalFetcher instance
        start_date: Start date
        end_date: End date
        bar_size: Bar size ('1 day', '1 hour', etc.)
        spot_config: Dict with 'symbol', 'exchange', 'currency' keys.
                     Defaults to BTC on PAXOS in USD.

    Returns:
        List of dicts with date and spot_price
    """
    import time
    from ib_insync import Crypto

    if spot_config is None:
        spot_config = {"symbol": "BTC", "exchange": "PAXOS", "currency": "USD"}

    try:
        contract = Crypto(spot_config["symbol"], spot_config["exchange"], spot_config["currency"])
        fetcher.ib.qualifyContracts(contract)

        print(f"    Fetching {spot_config['symbol']}.{spot_config['currency']} spot from {spot_config['exchange']}...")

        # Chunk into <= 365-day segments to stay within IBKR limits
        max_days = 365
        total_days = (end_date - start_date).days
        chunks = []
        chunk_start = start_date
        while chunk_start < end_date:
            chunk_end = min(chunk_start + timedelta(days=max_days), end_date)
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end

        result = []
        seen_dates = set()

        for i, (c_start, c_end) in enumerate(chunks):
            duration_days = (c_end - c_start).days
            if duration_days <= 0:
                continue
            duration_str = f"{duration_days} D"

            if len(chunks) > 1:
                print(f"    Chunk {i + 1}/{len(chunks)}: {c_start.date()} to {c_end.date()} ({duration_days}d)")

            bars = fetcher.ib.reqHistoricalData(
                contract,
                endDateTime=c_end,
                durationStr=duration_str,
                barSizeSetting=bar_size,
                whatToShow="MIDPOINT",
                useRTH=False,
                formatDate=1,
            )

            for bar in bars:
                if isinstance(bar.date, datetime):
                    date_obj = bar.date
                else:
                    date_obj = datetime.combine(bar.date, datetime.min.time())

                # Deduplicate across chunks
                date_key = date_obj.date()
                if date_key not in seen_dates:
                    seen_dates.add(date_key)
                    result.append({
                        "date": date_obj,
                        "spot_price": bar.close,
                    })

            # Rate limiting between chunks
            if i < len(chunks) - 1:
                time.sleep(1)

        result.sort(key=lambda x: x["date"])
        print(f"[OK] Fetched {len(result)} spot bars from IBKR ({spot_config['symbol']}.{spot_config['currency']} {spot_config['exchange']})")
        return result

    except Exception as e:
        print(f"[X] Failed to fetch IBKR spot history: {e}")
        return []


def merge_continuous_data(spot_data, cont_data, start_date, end_date):
    """
    Merge spot and continuous futures data by date.

    Computes basis metrics using the continuous futures price and
    the front-month expiry schedule for days-to-expiry calculations.
    """
    cont_by_date = {}
    for entry in cont_data:
        cont_by_date[entry["date"].date()] = entry["futures_price"]

    # Build expiry schedule for days-to-expiry calculation
    expiry_schedule = generate_expiry_schedule(start_date, end_date + timedelta(days=60))

    result = []
    for spot_entry in spot_data:
        date_key = spot_entry["date"].date()
        if date_key not in cont_by_date:
            continue

        spot_price = spot_entry["spot_price"]
        futures_price = cont_by_date[date_key]

        # Get front-month expiry for this date
        futures_expiry = get_front_month_expiry(spot_entry["date"], expiry_schedule)
        days_to_expiry = (futures_expiry - spot_entry["date"]).days

        basis_absolute = futures_price - spot_price
        basis_percent = (basis_absolute / spot_price) * 100 if spot_price else 0
        annualized_basis = (
            basis_percent * (365 / days_to_expiry) if days_to_expiry > 0 else 0
        )

        result.append({
            "date": spot_entry["date"],
            "spot_price": spot_price,
            "futures_price": futures_price,
            "basis_absolute": basis_absolute,
            "basis_percent": basis_percent,
            "annualized_basis": annualized_basis,
            "days_to_expiry": days_to_expiry,
            "futures_expiry": futures_expiry,
        })

    return result


def save_csv(data, output_file):
    """Export merged continuous data to CSV."""
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()

        for row in data:
            writer.writerow({
                "date": row["date"].strftime("%Y-%m-%d")
                if isinstance(row["date"], datetime)
                else row["date"],
                "spot_price": f"{row['spot_price']:.2f}",
                "futures_price": f"{row['futures_price']:.2f}",
                "basis_absolute": f"{row['basis_absolute']:.2f}",
                "basis_percent": f"{row['basis_percent']:.2f}",
                "annualized_basis": f"{row['annualized_basis']:.2f}",
                "days_to_expiry": row["days_to_expiry"],
                "futures_expiry": row["futures_expiry"].strftime("%Y-%m-%d")
                if isinstance(row["futures_expiry"], datetime)
                else row["futures_expiry"],
            })

    print(f"[OK] Saved {len(data)} rows to {output_file}")


def print_summary(data):
    """Print a formatted summary table."""
    if not data:
        print("[X] No data to display.")
        return

    header = (
        f"{'Date':<12} {'Spot':>12} {'Continuous':>12} "
        f"{'Basis':>10} {'Basis%':>8} {'Annual%':>9} {'DTE':>5} {'Expiry':<12}"
    )
    print(f"\n{header}")
    print("-" * len(header))

    for row in data:
        print(
            f"{row['date'].strftime('%Y-%m-%d'):<12} "
            f"{row['spot_price']:>12,.2f} "
            f"{row['futures_price']:>12,.2f} "
            f"{row['basis_absolute']:>10,.2f} "
            f"{row['basis_percent']:>7.2f}% "
            f"{row['annualized_basis']:>8.2f}% "
            f"{row['days_to_expiry']:>5d} "
            f"{row['futures_expiry'].strftime('%Y-%m-%d'):<12}"
        )

    # Print stats
    basis_pcts = [r["basis_percent"] for r in data]
    annual_pcts = [r["annualized_basis"] for r in data]
    print(f"\n--- Summary ({len(data)} data points) ---")
    print(f"  Spot range:       ${min(r['spot_price'] for r in data):,.2f} - ${max(r['spot_price'] for r in data):,.2f}")
    print(f"  Futures range:    ${min(r['futures_price'] for r in data):,.2f} - ${max(r['futures_price'] for r in data):,.2f}")
    print(f"  Basis % range:    {min(basis_pcts):.2f}% - {max(basis_pcts):.2f}%")
    print(f"  Avg basis %:      {sum(basis_pcts) / len(basis_pcts):.2f}%")
    print(f"  Avg annualized:   {sum(annual_pcts) / len(annual_pcts):.2f}%")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch historical continuous futures data from IBKR"
    )
    parser.add_argument(
        "--start", required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end", required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--pair",
        help="Investment pair from config (e.g. BTC, ETH). Default: config's default_pair",
    )
    parser.add_argument(
        "--symbol",
        help="Override futures symbol (default: from pair config)",
    )
    parser.add_argument(
        "--bar-size", default="1 day",
        help="Bar size: '1 day', '1 hour', '4 hours', etc. (default: '1 day')",
    )
    parser.add_argument(
        "--output", "-o",
        help="Output CSV path (default: data/<PAIR>_continuous_<start>_<end>.csv)",
    )
    parser.add_argument(
        "--config", "-c", default="config/config.json",
        help="Config file path",
    )
    parser.add_argument(
        "--futures-source", choices=["databento", "ibkr"], default="databento",
        help="Futures data source (default: databento)",
    )
    parser.add_argument(
        "--databento-dir",
        help="Databento data directory (default: from config or 'databento')",
    )
    parser.add_argument(
        "--no-csv", action="store_true",
        help="Skip CSV export, only print to console",
    )
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    if end_date <= start_date:
        print("[X] End date must be after start date.")
        sys.exit(1)

    # --- Resolve pair config ---
    config_loader = ConfigLoader(args.config)
    pair_name = args.pair or config_loader.default_pair
    pair_config = config_loader.get_pair(pair_name)
    spot_config = pair_config["spot"]
    futures_symbol = args.symbol or pair_config["futures"]["symbol"]
    futures_exchange = pair_config["futures"].get("exchange", "CME")

    # Resolve databento dir from CLI flag or config, with pair subfolder
    databento_base = args.databento_dir or config_loader.databento.get("data_dir", "databento")
    databento_dir = str(Path(databento_base) / pair_name)

    futures_label = "Databento" if args.futures_source == "databento" else f"IBKR ({futures_exchange} ContFuture)"

    print(f"\n*** Continuous Futures Fetcher ***")
    print(f"    Period:  {args.start} to {args.end}")
    print(f"    Pair:    {pair_name}")
    print(f"    Symbol:  {futures_symbol}")
    print(f"    Futures: {futures_label}")
    print(f"    Spot:    IBKR {spot_config['symbol']}.{spot_config['currency']} ({spot_config['exchange']})")
    print(f"    Bar:     {args.bar_size}\n")

    # --- Step 1: Connect to IBKR (only if needed) ---
    fetcher = None
    if args.futures_source == "ibkr":
        fetcher = IBKRHistoricalFetcher.from_config(config_loader.ibkr)
        if not fetcher.connect():
            print("[X] Failed to connect to IBKR. Is TWS/Gateway running?")
            sys.exit(1)

    # --- Step 2: Fetch continuous futures ---
    if args.futures_source == "databento":
        print("[1/2] Fetching continuous futures from Databento...")
        db_fetcher = DatabentoLocalFetcher(data_dir=databento_dir)
        cont_data = db_fetcher.get_historical_continuous_futures(
            symbol=futures_symbol,
            exchange=futures_exchange,
            start_date=start_date,
            end_date=end_date,
            bar_size=args.bar_size,
        )
    else:
        print("[1/2] Fetching continuous futures from IBKR...")
        cont_data = fetcher.get_historical_continuous_futures(
            symbol=futures_symbol,
            exchange=futures_exchange,
            start_date=start_date,
            end_date=end_date,
            bar_size=args.bar_size,
        )

    if not cont_data:
        if fetcher:
            fetcher.disconnect()
        print("[X] No continuous futures data returned.")
        sys.exit(1)

    # --- Step 3: Fetch spot from IBKR ---
    # Spot always comes from IBKR, so connect if not already connected
    if fetcher is None:
        fetcher = IBKRHistoricalFetcher.from_config(config_loader.ibkr)
        if not fetcher.connect():
            print("[X] Failed to connect to IBKR for spot data. Is TWS/Gateway running?")
            sys.exit(1)

    print(f"[2/2] Fetching spot prices from IBKR ({spot_config['symbol']}.{spot_config['currency']} {spot_config['exchange']})...")
    spot_data = fetch_ibkr_spot_history(
        fetcher=fetcher,
        start_date=start_date,
        end_date=end_date,
        bar_size=args.bar_size,
        spot_config=spot_config,
    )

    fetcher.disconnect()

    if not spot_data:
        print("[X] No spot data returned from IBKR.")
        sys.exit(1)

    # --- Step 4: Merge and compute basis ---
    data = merge_continuous_data(spot_data, cont_data, start_date, end_date)

    if not data:
        print("[X] No overlapping dates between spot and futures data.")
        sys.exit(1)

    print(f"\n[OK] Merged {len(data)} data points")

    # --- Step 5: Display ---
    print_summary(data)

    # --- Step 6: Export to CSV ---
    if not args.no_csv:
        output_file = args.output or f"data/{pair_name}_continuous_{args.start}_{args.end}.csv"
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        save_csv(data, output_file)


if __name__ == "__main__":
    main()
