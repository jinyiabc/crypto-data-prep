#!/usr/bin/env python3
"""
Crypto Data Prep - Data preparation toolkit for cryptocurrency trading.

Usage:
    python main.py fetch-spot           # Fetch BTC spot prices
    python main.py fetch-futures        # Fetch BTC futures from IBKR
    python main.py fetch-historical     # Fetch historical data from IBKR
    python main.py fetch-historical --source binance --expiry 202506  # Futures from Binance
    python main.py backtest --data FILE # Run backtest on CSV data
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path for package imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from crypto_data.data.coinbase import CoinbaseFetcher
from crypto_data.data.binance import BinanceFetcher
from crypto_data.utils.config import ConfigLoader


def cmd_fetch_spot(args):
    """Fetch spot prices from multiple sources."""
    print("\n*** Crypto Spot Price Fetcher ***\n")

    # Coinbase
    print("[1/2] Fetching from Coinbase...")
    coinbase = CoinbaseFetcher()
    cb_price = coinbase.fetch_spot_price()
    if cb_price:
        print(f"  Coinbase BTC-USD: ${cb_price:,.2f}")
    else:
        print("  Coinbase: Failed")

    # Binance
    print("[2/2] Fetching from Binance...")
    binance = BinanceFetcher()
    bn_price = binance.fetch_spot_price()
    if bn_price:
        print(f"  Binance BTCUSDT: ${bn_price:,.2f}")
    else:
        print("  Binance: Failed")

    if cb_price and bn_price:
        diff = abs(cb_price - bn_price)
        diff_pct = (diff / cb_price) * 100
        print(f"\n  Spread: ${diff:.2f} ({diff_pct:.3f}%)")


def cmd_fetch_futures(args):
    """Fetch futures data from IBKR."""
    print("\n*** IBKR Futures Fetcher ***\n")

    try:
        from crypto_data.data.ibkr import IBKRFetcher

        config_loader = ConfigLoader(args.config)
        ibkr_config = config_loader.ibkr

        ibkr = IBKRFetcher.from_config(ibkr_config)

        if ibkr.connect():
            print("[OK] Connected to IBKR\n")

            data = ibkr.get_complete_basis_data()

            if data:
                print(f"\nSpot:    ${data['spot_price']:,.2f} ({data['spot_source']})")
                print(f"Futures: ${data['futures_price']:,.2f} ({data['futures_local_symbol']})")
                print(f"Basis:   {data['basis_percent']:.2f}%")
                print(f"Monthly: {data['monthly_basis']:.2f}%")
                print(f"Annual:  {data['annualized_basis']:.2f}%")
                print(f"Expiry:  {data['days_to_expiry']} days")

            ibkr.disconnect()
        else:
            print("[X] Failed to connect to IBKR")

    except ImportError:
        print("[X] ib-insync not installed. Run: pip install ib-insync")


def cmd_fetch_historical(args):
    """Fetch historical data from IBKR or Binance."""
    import csv

    source = args.source.lower()

    if source == "binance" and args.expiry:
        # Fetch historical futures from Binance
        print("\n*** Binance Historical Futures Fetcher ***\n")

        binance = BinanceFetcher()
        expiry = args.expiry

        print(f"Fetching {args.days} days of BTCUSD quarterly futures (expiry {expiry})...")
        data = binance.get_historical_futures_klines(
            expiry=expiry,
            days=args.days,
        )

        if data:
            print(f"[OK] Fetched {len(data)} bars")

            output_file = args.output or f"data/BTC_futures_{expiry}_{args.days}d.csv"
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)

            fieldnames = ["date", "open", "high", "low", "close", "volume", "futures_price", "expiry"]
            with open(output_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for bar in data:
                    row = dict(bar)
                    row["date"] = bar["date"].strftime("%Y-%m-%d %H:%M:%S") if isinstance(bar["date"], datetime) else bar["date"]
                    writer.writerow(row)

            print(f"[OK] Saved to {output_file}")
        else:
            print("[X] No data returned. The contract may be expired or not yet listed.")
            available = binance.list_available_contracts()
            if available:
                print(f"    Available contracts: {', '.join(available)}")

    else:
        # Fetch historical ETF data from IBKR (original behavior)
        print("\n*** IBKR Historical Data Fetcher ***\n")

        try:
            from crypto_data.data.ibkr import IBKRHistoricalFetcher

            config_loader = ConfigLoader(args.config)
            ibkr_config = config_loader.ibkr

            fetcher = IBKRHistoricalFetcher.from_config(ibkr_config)

            if fetcher.connect():
                print("[OK] Connected to IBKR\n")

                end_date = datetime.now()
                start_date = end_date - timedelta(days=args.days)

                print(f"Fetching {args.days} days of {args.symbol} data...")
                etf_data = fetcher.get_historical_spot(
                    symbol=args.symbol,
                    start_date=start_date,
                    end_date=end_date,
                )

                if etf_data:
                    print(f"[OK] Fetched {len(etf_data)} bars")

                    output_file = args.output or f"data/{args.symbol}_{args.days}d.csv"
                    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

                    with open(output_file, "w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=["date", "open", "high", "low", "close", "volume", "etf_price", "btc_price"])
                        writer.writeheader()
                        for bar in etf_data:
                            writer.writerow(bar)

                    print(f"[OK] Saved to {output_file}")

                fetcher.disconnect()
            else:
                print("[X] Failed to connect to IBKR")

        except ImportError:
            print("[X] ib-insync not installed. Run: pip install ib-insync")


def cmd_backtest(args):
    """Run backtest on historical data."""
    print("\n*** Backtester ***\n")

    if not args.data:
        print("[X] Please provide data file with --data")
        return

    from crypto_data.backtest.engine import Backtester

    config_loader = ConfigLoader(args.config)

    # Create a minimal config for backtesting
    backtest_config = type("Config", (), {
        "account_size": config_loader.get("account_size", 200000),
        "spot_target_pct": config_loader.get("spot_target_pct", 0.5),
        "futures_target_pct": config_loader.get("futures_target_pct", 0.5),
        "funding_cost_annual": config_loader.get("funding_cost_annual", 0.05),
        "leverage": config_loader.get("leverage", 1.0),
        "cme_contract_size": config_loader.get("cme_contract_size", 5.0),
        "min_monthly_basis": config_loader.get("min_monthly_basis", 0.005),
    })()

    backtester = Backtester(backtest_config)

    print(f"Loading data from {args.data}...")
    data = backtester.load_historical_data(args.data)

    print(f"Running backtest on {len(data)} data points...")
    result = backtester.run_backtest(data, holding_days=args.holding_days)

    # Trade log
    if result.trades:
        print(f"\n{'='*100}")
        print("TRADE LOG")
        print(f"{'='*100}")
        header = (
            f"{'#':>3}  {'Entry Date':<12} {'Exit Date':<12} {'Status':<13} "
            f"{'Entry Basis':>12} {'Exit Basis':>11} "
            f"{'Days':>5} {'Return%':>8} {'P&L':>12}"
        )
        print(header)
        print("-" * 100)
        for i, trade in enumerate(result.trades, 1):
            exit_date = trade.exit_date.strftime("%Y-%m-%d") if trade.exit_date else "-"
            exit_basis = f"{trade.exit_basis:>11,.2f}" if trade.exit_basis is not None else "          -"
            return_pct = f"{trade.return_pct * 100:>7.2f}%" if trade.return_pct is not None else "       -"
            pnl = f"${trade.realized_pnl:>11,.2f}" if trade.realized_pnl is not None else "          -"
            print(
                f"{i:>3}  {trade.entry_date.strftime('%Y-%m-%d'):<12} {exit_date:<12} {trade.status:<13} "
                f"{trade.entry_basis:>12,.2f} {exit_basis} "
                f"{trade.holding_days:>5} {return_pct} {pnl}"
            )

    # Summary
    print(f"\n{'='*50}")
    print("BACKTEST RESULTS")
    print(f"{'='*50}")
    print(f"Period:          {result.start_date.strftime('%Y-%m-%d')} to {result.end_date.strftime('%Y-%m-%d')}")
    print(f"Total Return:    {result.total_return:.2%}")
    print(f"Sharpe Ratio:    {result.sharpe_ratio:.2f}")
    print(f"Max Drawdown:    {result.max_drawdown:.2%}")
    print(f"Win Rate:        {result.win_rate:.2%} ({result.winning_trades}W / {result.losing_trades}L)")
    print(f"Total Trades:    {result.total_trades}")
    if result.avg_win:
        print(f"Avg Win:         {result.avg_win:.2%}")
    if result.avg_loss:
        print(f"Avg Loss:        {result.avg_loss:.2%}")
    if result.total_trades > 0:
        print(f"Profit Factor:   {result.profit_factor:.2f}")
    print(f"Initial Capital: ${result.initial_capital:,.2f}")
    print(f"Final Capital:   ${result.final_capital:,.2f}")


def main():
    parser = argparse.ArgumentParser(
        description="Crypto Data Prep - Data preparation toolkit"
    )
    parser.add_argument(
        "--config", "-c",
        default="config/config.json",
        help="Path to config file",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # fetch-spot
    subparsers.add_parser("fetch-spot", help="Fetch spot prices")

    # fetch-futures
    subparsers.add_parser("fetch-futures", help="Fetch futures from IBKR")

    # fetch-historical
    hist_parser = subparsers.add_parser("fetch-historical", help="Fetch historical data")
    hist_parser.add_argument("--source", default="ibkr", choices=["ibkr", "binance"], help="Data source (default: ibkr)")
    hist_parser.add_argument("--symbol", default="IBIT", help="ETF symbol (for IBKR)")
    hist_parser.add_argument("--expiry", help="Futures expiry YYYYMM (e.g., 202506, for Binance)")
    hist_parser.add_argument("--days", type=int, default=90, help="Days of history")
    hist_parser.add_argument("--output", "-o", help="Output CSV file")

    # backtest
    bt_parser = subparsers.add_parser("backtest", help="Run backtest")
    bt_parser.add_argument("--data", "-d", required=True, help="CSV data file")
    bt_parser.add_argument("--holding-days", type=int, default=30, help="Holding period")

    args = parser.parse_args()

    if args.command == "fetch-spot":
        cmd_fetch_spot(args)
    elif args.command == "fetch-futures":
        cmd_fetch_futures(args)
    elif args.command == "fetch-historical":
        cmd_fetch_historical(args)
    elif args.command == "backtest":
        cmd_backtest(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
