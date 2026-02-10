#!/usr/bin/env python3
"""Accumulate and export futures + spot price data over a date range."""

import csv
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from crypto_data.data.ibkr import IBKRHistoricalFetcher
from crypto_data.utils.expiry import (
    get_last_friday_of_month,
    get_front_month_expiry_str,
    generate_expiry_schedule,
    get_front_month_expiry,
)
from crypto_data.utils.logging import LoggingMixin


CSV_FIELDNAMES = [
    "date",
    "spot_price",
    "futures_price",
    "future_continuous",
    "futures_expiry",
    "basis_absolute",
    "basis_percent",
    "annualized_basis",
    "days_to_expiry",
]


class FuturesAccumulator(LoggingMixin):
    """Accumulate futures and spot price data from IBKR + Binance over a date range."""

    BINANCE_SPOT_API = "https://api.binance.com/api/v3"

    def __init__(self, fetcher: IBKRHistoricalFetcher):
        self.fetcher = fetcher

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "FuturesAccumulator":
        """Create FuturesAccumulator from IBKR config dict."""
        fetcher = IBKRHistoricalFetcher.from_config(config)
        return cls(fetcher)

    def _fetch_binance_spot_history(
        self,
        start_date: datetime,
        end_date: datetime,
        spot_symbol: str = "BTCUSDT",
        interval: str = "1d",
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical spot prices from Binance klines API.

        Args:
            start_date: Start date
            end_date: End date
            spot_symbol: Binance spot pair (default: BTCUSDT)
            interval: Kline interval (default: 1d)

        Returns:
            List of dicts with date and spot_price (close)
        """
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)
        result = []
        current_start = start_ms

        try:
            while current_start < end_ms:
                url = f"{self.BINANCE_SPOT_API}/klines"
                params = {
                    "symbol": spot_symbol,
                    "interval": interval,
                    "startTime": current_start,
                    "endTime": end_ms,
                    "limit": 1000,
                }
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                klines = response.json()

                if not klines:
                    break

                for k in klines:
                    open_time = datetime.fromtimestamp(k[0] / 1000)
                    result.append({
                        "date": open_time,
                        "spot_price": float(k[4]),  # close price
                    })

                current_start = int(klines[-1][6]) + 1  # closeTime + 1ms

                if len(klines) < 1000:
                    break

            self.log(f"[OK] Fetched {len(result)} spot bars from Binance")
            return result

        except Exception as e:
            self.log(f"[X] Failed to fetch Binance spot history: {e}")
            return []

    def accumulate(
        self,
        start_date: datetime,
        end_date: datetime,
        expiry: str = None,
        symbol: str = "MBT",
        spot_symbol: str = "BTCUSDT",
        bar_size: str = "1 day",
    ) -> List[Dict[str, Any]]:
        """
        Accumulate futures and spot price data between start and end date.

        Fetches historical futures from IBKR and spot prices from Binance,
        merges them by date, and computes basis calculations for each day.

        Args:
            start_date: Start date for historical data
            end_date: End date for historical data
            expiry: Futures contract expiry (YYYYMM), None = front-month
            symbol: Futures symbol ('MBT' or 'BTC')
            spot_symbol: Binance spot pair (default: 'BTCUSDT')
            bar_size: IBKR bar size ('1 day', '1 hour', etc.)

        Returns:
            List of dicts sorted by date, each containing:
            date, spot_price, futures_price, futures_expiry,
            basis_absolute, basis_percent, annualized_basis, days_to_expiry
        """
        if not self.fetcher.connected:
            if not self.fetcher.connect():
                return []

        if expiry is None:
            expiry = get_front_month_expiry_str()

        self.log(f"[*] Accumulating futures data: {start_date.date()} to {end_date.date()}")
        self.log(f"    Contract: {symbol} {expiry}, Spot: Binance {spot_symbol}")

        spot_data = self._fetch_binance_spot_history(
            start_date=start_date,
            end_date=end_date,
            spot_symbol=spot_symbol,
        )

        if not spot_data:
            self.log("[X] Failed to get spot data")
            return []

        futures_data = self.fetcher.get_historical_futures(
            expiry=expiry,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            bar_size=bar_size,
        )

        if not futures_data:
            self.log("[X] Failed to get futures data")
            return []

        futures_by_date = {}
        for entry in futures_data:
            date_key = entry["date"].date()
            futures_by_date[date_key] = entry

        expiry_date = get_last_friday_of_month(int(expiry[:4]), int(expiry[4:6]))

        result = []
        for spot_entry in spot_data:
            date_key = spot_entry["date"].date()
            if date_key not in futures_by_date:
                continue

            futures_entry = futures_by_date[date_key]
            spot_price = spot_entry["spot_price"]
            futures_price = futures_entry["futures_price"]
            futures_expiry = futures_entry.get("expiry") or expiry_date

            basis_absolute = futures_price - spot_price
            basis_percent = (basis_absolute / spot_price) * 100 if spot_price else 0
            days_to_expiry = (futures_expiry - spot_entry["date"]).days

            annualized_basis = (
                basis_percent * (365 / days_to_expiry) if days_to_expiry > 0 else 0
            )

            result.append({
                "date": spot_entry["date"],
                "spot_price": spot_price,
                "futures_price": futures_price,
                "futures_expiry": futures_expiry,
                "basis_absolute": basis_absolute,
                "basis_percent": basis_percent,
                "annualized_basis": annualized_basis,
                "days_to_expiry": days_to_expiry,
            })

        self.log(f"[OK] Accumulated {len(result)} data points")
        return result

    def accumulate_continuous(
        self,
        start_date: datetime,
        end_date: datetime,
        symbol: str = "MBT",
        spot_symbol: str = "BTCUSDT",
        bar_size: str = "1 day",
    ) -> List[Dict[str, Any]]:
        """
        Accumulate futures data with both front-month contract and continuous futures.

        Uses the front-month contract at start_date for futures_price and basis
        calculations, and IBKR ContFuture for the future_continuous column.

        Args:
            start_date: Start date for historical data
            end_date: End date for historical data
            symbol: Futures symbol ('MBT' or 'BTC')
            spot_symbol: Binance spot pair (default: 'BTCUSDT')
            bar_size: IBKR bar size ('1 day', '1 hour', etc.)

        Returns:
            List of dicts sorted by date, each containing:
            date, spot_price, futures_price, future_continuous, futures_expiry,
            basis_absolute, basis_percent, annualized_basis, days_to_expiry
        """
        if not self.fetcher.connected:
            if not self.fetcher.connect():
                return []

        # Determine front-month contract at start_date, with fallback
        expiry_schedule = generate_expiry_schedule(start_date, end_date)
        front = get_front_month_expiry(start_date, expiry_schedule)

        # Build candidate list: front-month first, then subsequent months
        candidates = sorted(
            [dt for dt in expiry_schedule if dt >= front],
            key=lambda dt: dt,
        )

        self.log(f"[*] Accumulating continuous futures: {start_date.date()} to {end_date.date()}")
        self.log(f"    Symbol: {symbol}, Spot: Binance {spot_symbol}")

        # Fetch spot data from Binance
        spot_data = self._fetch_binance_spot_history(
            start_date=start_date,
            end_date=end_date,
            spot_symbol=spot_symbol,
        )

        if not spot_data:
            self.log("[X] Failed to get spot data")
            return []

        # Fetch front-month contract, fall back to next if expired/unavailable
        futures_data = []
        expiry_str = None
        for candidate in candidates:
            expiry_str = f"{candidate.year:04d}{candidate.month:02d}"
            self.log(f"[*] Trying contract {symbol} {expiry_str}...")
            futures_data = self.fetcher.get_historical_futures(
                expiry=expiry_str,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                bar_size=bar_size,
            )
            if futures_data:
                self.log(f"[OK] Using contract {expiry_str}")
                break
            self.log(f"[!] No data for {expiry_str}, trying next...")

        if not futures_data:
            self.log("[X] Failed to get futures data for any contract")
            return []

        futures_by_date = {}
        for entry in futures_data:
            futures_by_date[entry["date"].date()] = entry

        expiry_date = get_last_friday_of_month(int(expiry_str[:4]), int(expiry_str[4:6]))

        # Fetch continuous futures from IBKR ContFuture
        cont_data = self.fetcher.get_historical_continuous_futures(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            bar_size=bar_size,
        )
        cont_by_date = {}
        for entry in cont_data:
            cont_by_date[entry["date"].date()] = entry["futures_price"]

        # Merge spot + futures + continuous
        result = []
        for spot_entry in spot_data:
            date_key = spot_entry["date"].date()
            if date_key not in futures_by_date:
                continue

            futures_entry = futures_by_date[date_key]
            spot_price = spot_entry["spot_price"]
            futures_price = futures_entry["futures_price"]
            futures_expiry = futures_entry.get("expiry") or expiry_date

            basis_absolute = futures_price - spot_price
            basis_percent = (basis_absolute / spot_price) * 100 if spot_price else 0
            days_to_expiry = (futures_expiry - spot_entry["date"]).days

            annualized_basis = (
                basis_percent * (365 / days_to_expiry) if days_to_expiry > 0 else 0
            )

            result.append({
                "date": spot_entry["date"],
                "spot_price": spot_price,
                "futures_price": futures_price,
                "future_continuous": cont_by_date.get(date_key),
                "futures_expiry": futures_expiry,
                "basis_absolute": basis_absolute,
                "basis_percent": basis_percent,
                "annualized_basis": annualized_basis,
                "days_to_expiry": days_to_expiry,
            })

        self.log(f"[OK] Accumulated {len(result)} continuous data points")
        return result

    def to_csv(
        self,
        data: List[Dict[str, Any]],
        output_file: str,
    ) -> None:
        """
        Export accumulated futures data to CSV.

        Args:
            data: List of dicts returned by accumulate() or accumulate_continuous()
            output_file: Output CSV file path
        """
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()

            for row in data:
                cont_price = row.get("future_continuous")
                writer.writerow({
                    "date": row["date"].strftime("%Y-%m-%d")
                    if isinstance(row["date"], datetime)
                    else row["date"],
                    "spot_price": f"{row['spot_price']:.2f}",
                    "futures_price": f"{row['futures_price']:.2f}",
                    "future_continuous": f"{cont_price:.2f}" if cont_price is not None else "",
                    "futures_expiry": row["futures_expiry"].strftime("%Y-%m-%d")
                    if isinstance(row["futures_expiry"], datetime)
                    else row["futures_expiry"],
                    "basis_absolute": f"{row['basis_absolute']:.2f}",
                    "basis_percent": f"{row['basis_percent']:.2f}",
                    "annualized_basis": f"{row['annualized_basis']:.2f}",
                    "days_to_expiry": row["days_to_expiry"],
                })

        self.log(f"[OK] Saved {len(data)} rows to {output_file}")
