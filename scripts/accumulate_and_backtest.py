#!/usr/bin/env python3
"""
Accumulate futures basis data and run backtest in one step.

Date range: start = previous expiry date, end = current expiry date - 1.

Requirements:
    - For --futures-source databento (default): Databento CSV in databento/<PAIR>/ folder
    - For --futures-source ibkr or spot from IBKR: TWS or IB Gateway running
    - pip install -e ".[ibkr]"  (only when using IBKR)

Usage:
    python scripts/accumulate_and_backtest.py --expiry 202402
    python scripts/accumulate_and_backtest.py --year 2024
    python scripts/accumulate_and_backtest.py --expiry 202402 --pair ETH
    python scripts/accumulate_and_backtest.py --expiry 202603 --holding-days 15
    python scripts/accumulate_and_backtest.py --futures-source ibkr --holding-days 30
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_data.backtest.engine import Backtester
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
    parser = argparse.ArgumentParser(description="Accumulate futures basis data and run backtest")
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
    parser.add_argument("--holding-days", type=int, default=30, help="Backtest holding period (default: 30)")
    parser.add_argument("--entry-threshold", type=float, default=0.005,
                        help="Monthly basis entry threshold as decimal (default: 0.005 = 0.5%%)")
    parser.add_argument("--stop-loss-threshold", type=float, default=0.002,
                        help="Monthly basis stop-loss threshold as decimal (default: 0.002 = 0.2%%)")
    parser.add_argument("--exit-threshold", type=float, default=0.035,
                        help="Monthly basis exit threshold as decimal (default: 0.035 = 3.5%%)")
    parser.add_argument("--params", help="Load signal params from JSON file (from optimize_signals.py --save-params)")
    parser.add_argument("--config", "-c", default="config/config.json", help="Config file path")
    args = parser.parse_args()

    # Load optimized params from JSON (overrides defaults, explicit CLI flags take priority)
    if args.params:
        with open(args.params) as f:
            params = json.load(f)
        defaults = parser.parse_args([])
        if args.entry_threshold == defaults.entry_threshold and "entry_threshold" in params:
            args.entry_threshold = params["entry_threshold"]
        if args.stop_loss_threshold == defaults.stop_loss_threshold and "stop_loss_threshold" in params:
            args.stop_loss_threshold = params["stop_loss_threshold"]
        if args.exit_threshold == defaults.exit_threshold and "exit_threshold" in params:
            args.exit_threshold = params["exit_threshold"]
        if args.holding_days == defaults.holding_days and "holding_days" in params:
            args.holding_days = params["holding_days"]

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
    needs_ibkr = (args.futures_source == "ibkr")
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

    # --- Step 1: Accumulate ---
    label = str(args.year) if args.year else format_contract_name(futures_symbol, expiry_list[0])
    print(f"\n*** Accumulate + Backtest: {pair_name} {label} ***")
    print(f"    Futures: {'Databento' if args.futures_source == 'databento' else 'IBKR'}\n")

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

    # Print basis table
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

    # --- Step 2: Backtest ---
    backtest_config = type("Config", (), {
        "account_size": config_loader.get("account_size", 200000),
        "funding_cost_annual": config_loader.get("funding_cost_annual", 0.05),
        "entry_threshold": args.entry_threshold,
        "stop_loss_threshold": args.stop_loss_threshold,
        "exit_threshold": args.exit_threshold,
    })()

    backtester = Backtester(backtest_config)
    bt_data = backtester.load_historical_data(output_file)

    print(f"\nRunning backtest on {len(bt_data)} data points (holding: {args.holding_days}d, "
          f"entry: {args.entry_threshold:.1%}, stop: {args.stop_loss_threshold:.1%}, exit: {args.exit_threshold:.1%})...")
    result = backtester.run_backtest(bt_data, holding_days=args.holding_days)

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


if __name__ == "__main__":
    main()
