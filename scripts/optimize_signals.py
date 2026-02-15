#!/usr/bin/env python3
"""
Grid search optimizer for backtest signal thresholds.

Sweeps entry, stop-loss, exit thresholds and holding days to find the
parameter combination that maximizes total return.

Usage:
    # Optimize on pre-existing CSV
    python scripts/optimize_signals.py --data data/BTC_futures_basis_202402.csv

    # Accumulate then optimize
    python scripts/optimize_signals.py --expiry 202402
    python scripts/optimize_signals.py --year 2024
    python scripts/optimize_signals.py --year 2024 --pair ETH

    # Show more results
    python scripts/optimize_signals.py --data data/BTC_futures_basis_202402.csv --top 30
"""

import argparse
import json
import sys
from datetime import timedelta
from itertools import product
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_data.backtest.engine import Backtester
from crypto_data.data.accumulator import FuturesAccumulator, format_contract_name
from crypto_data.utils.config import ConfigLoader
from crypto_data.utils.expiry import get_front_month_expiry_str, get_last_friday_of_month


def get_date_range(expiry_str, end_on_expiry=False):
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


def frange(start, stop, step):
    """Float range generator."""
    values = []
    val = start
    while val <= stop + step / 10:
        values.append(round(val, 6))
        val += step
    return values


def run_optimization(bt_data, account_size, funding_cost_annual, top_n=20, save_params=None):
    """Run grid search over signal thresholds and holding days."""

    # Parameter grid
    entry_values = frange(0.002, 0.020, 0.002)
    stop_values = frange(0.001, 0.005, 0.001)
    exit_values = frange(0.020, 0.060, 0.005)
    holding_values = [10, 20, 30, 40, 50, 60]

    total_combos = len(entry_values) * len(stop_values) * len(exit_values) * len(holding_values)
    print(f"\nGrid search: {total_combos} combinations "
          f"({len(entry_values)} entry x {len(stop_values)} stop x "
          f"{len(exit_values)} exit x {len(holding_values)} hold)")

    results = []
    for i, (entry, stop, exit_t, hold) in enumerate(
        product(entry_values, stop_values, exit_values, holding_values)
    ):
        # Skip invalid: entry must be above stop_loss
        if entry <= stop:
            continue
        # Skip invalid: exit must be above entry
        if exit_t <= entry:
            continue

        config = type("Config", (), {
            "account_size": account_size,
            "funding_cost_annual": funding_cost_annual,
            "entry_threshold": entry,
            "stop_loss_threshold": stop,
            "exit_threshold": exit_t,
        })()

        backtester = Backtester(config)
        result = backtester.run_backtest(bt_data, holding_days=hold)

        results.append({
            "entry": entry,
            "stop": stop,
            "exit": exit_t,
            "hold": hold,
            "return": result.total_return,
            "sharpe": result.sharpe_ratio,
            "max_dd": result.max_drawdown,
            "trades": result.total_trades,
            "win_rate": result.win_rate,
            "wins": result.winning_trades,
            "losses": result.losing_trades,
        })

    # Also run with default params for comparison
    default_config = type("Config", (), {
        "account_size": account_size,
        "funding_cost_annual": funding_cost_annual,
        "entry_threshold": 0.005,
        "stop_loss_threshold": 0.002,
        "exit_threshold": 0.035,
    })()
    default_bt = Backtester(default_config)
    default_result = default_bt.run_backtest(bt_data, holding_days=30)

    # Sort by total return descending
    results.sort(key=lambda x: x["return"], reverse=True)

    valid = [r for r in results if r["trades"] > 0]
    print(f"Valid combinations (trades > 0): {len(valid)} / {len(results)}\n")

    # Print top results
    print(f"{'Rank':>4}  {'Entry%':>7} {'Stop%':>6} {'Exit%':>6} {'Hold':>5} "
          f"{'Return%':>8} {'Sharpe':>7} {'MaxDD%':>7} {'Trades':>7} {'WinRate':>8}")
    print("-" * 80)

    for i, r in enumerate(valid[:top_n], 1):
        print(
            f"{i:>4}  {r['entry']*100:>6.1f}% {r['stop']*100:>5.1f}% "
            f"{r['exit']*100:>5.1f}% {r['hold']:>5} "
            f"{r['return']*100:>7.2f}% {r['sharpe']:>7.2f} "
            f"{-r['max_dd']*100:>6.2f}% {r['trades']:>7} "
            f"{r['win_rate']*100:>6.1f}%"
        )

    # Default comparison
    print(f"\n{'Default params':>40}: entry=0.5%, stop=0.2%, exit=3.5%, hold=30")
    print(f"{'Default result':>40}: return={default_result.total_return*100:.2f}%, "
          f"sharpe={default_result.sharpe_ratio:.2f}, "
          f"trades={default_result.total_trades}, "
          f"win_rate={default_result.win_rate*100:.1f}%")

    if valid:
        best = valid[0]
        print(f"\n{'Best params':>40}: entry={best['entry']*100:.1f}%, "
              f"stop={best['stop']*100:.1f}%, exit={best['exit']*100:.1f}%, hold={best['hold']}")
        print(f"{'Best result':>40}: return={best['return']*100:.2f}%, "
              f"sharpe={best['sharpe']:.2f}, "
              f"trades={best['trades']}, "
              f"win_rate={best['win_rate']*100:.1f}%")

        if save_params:
            params = {
                "entry_threshold": best["entry"],
                "stop_loss_threshold": best["stop"],
                "exit_threshold": best["exit"],
                "holding_days": best["hold"],
            }
            Path(save_params).parent.mkdir(parents=True, exist_ok=True)
            with open(save_params, "w") as f:
                json.dump(params, f, indent=2)
            print(f"\nSaved best params to {save_params}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Optimize backtest signal thresholds via grid search")

    # Data source: either pre-existing CSV or accumulate
    data_group = parser.add_mutually_exclusive_group()
    data_group.add_argument("--data", help="Pre-existing CSV file path (skip accumulation)")
    data_group.add_argument("--expiry", help="Futures expiry YYYYMM to accumulate")
    data_group.add_argument("--year", type=int, help="Accumulate all 12 months of a year (e.g. 2024)")

    parser.add_argument("--pair", help="Investment pair from config (e.g. BTC, ETH)")
    parser.add_argument("--futures-source", choices=["databento", "ibkr"], default="databento",
                        help="Futures data source (default: databento)")
    parser.add_argument("--databento-dir", help="Databento data directory")
    parser.add_argument("--end-on-expiry", action="store_true",
                        help="Date range: prev expiry+1 to curr expiry")
    parser.add_argument("--save-params", help="Save best params to JSON file (e.g. data/best_params.json)")
    parser.add_argument("--top", type=int, default=20, help="Number of top results to show (default: 20)")
    parser.add_argument("--config", "-c", default="config/config.json", help="Config file path")
    args = parser.parse_args()

    config_loader = ConfigLoader(args.config)
    account_size = config_loader.get("account_size", 200000)
    funding_cost_annual = config_loader.get("funding_cost_annual", 0.05)

    if args.data:
        # Use pre-existing CSV
        csv_path = args.data
        print(f"\n*** Signal Optimizer: {csv_path} ***")
    else:
        # Accumulate data first
        acc = FuturesAccumulator.from_config(config_loader.ibkr)
        pair_name = args.pair or config_loader.default_pair
        pair_config = config_loader.get_pair(pair_name)
        spot_config = pair_config["spot"]
        futures_symbol = pair_config["futures"]["symbol"]
        futures_exchange = pair_config["futures"].get("exchange", "CME")

        databento_base = args.databento_dir or config_loader.databento.get("data_dir", "databento")
        databento_dir = str(Path(databento_base) / pair_name)

        needs_ibkr = (args.futures_source == "ibkr")
        if needs_ibkr:
            if not acc.fetcher.connect():
                print("[X] Failed to connect to IBKR.")
                sys.exit(1)

        if args.year:
            expiry_list = [f"{args.year}{m:02d}" for m in range(1, 13)]
            label = str(args.year)
        elif args.expiry:
            expiry_list = [args.expiry]
            label = format_contract_name(futures_symbol, args.expiry)
        else:
            expiry_list = [get_front_month_expiry_str()]
            label = format_contract_name(futures_symbol, expiry_list[0])

        print(f"\n*** Signal Optimizer: {pair_name} {label} ***")

        all_data = []
        for expiry_str in expiry_list:
            start_date, end_date = get_date_range(expiry_str, args.end_on_expiry)
            contract_name = format_contract_name(futures_symbol, expiry_str)

            if len(expiry_list) > 1:
                print(f"  Accumulating {contract_name}...")

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

        if needs_ibkr:
            acc.fetcher.disconnect()

        if not all_data:
            print("[X] No data returned.")
            sys.exit(1)

        # Save to temp CSV for backtester
        if args.year:
            csv_path = f"data/{pair_name}_futures_basis_{args.year}.csv"
        else:
            csv_path = f"data/{pair_name}_futures_basis_{expiry_list[0]}.csv"
        Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
        acc.to_csv(all_data, csv_path)
        print(f"Saved {len(all_data)} rows to {csv_path}")

    # Load data and run optimization
    backtester = Backtester()
    bt_data = backtester.load_historical_data(csv_path)
    print(f"Loaded {len(bt_data)} data points")

    run_optimization(bt_data, account_size, funding_cost_annual, top_n=args.top,
                     save_params=args.save_params)


if __name__ == "__main__":
    main()
