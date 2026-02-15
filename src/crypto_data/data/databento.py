#!/usr/bin/env python3
"""
Databento local CSV fetcher for CME Bitcoin futures historical data.

Reads pre-downloaded Databento OHLCV-1d CSV files and provides the same
interface as IBKRHistoricalFetcher for futures data.
"""

import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from crypto_data.utils.expiry import (
    generate_expiry_schedule,
    get_front_month_expiry,
    get_front_month_expiry_str,
    get_last_friday_of_month,
)
from crypto_data.utils.logging import LoggingMixin


# CME month codes: maps letter to month number
CME_MONTH_CODES = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}

# Reverse: month number to letter
MONTH_TO_CME_CODE = {v: k for k, v in CME_MONTH_CODES.items()}

# Base year for year-digit decoding.
# CME year digits cycle every 10 years. Digit "1" = 2021, ..., "6" = 2026.
YEAR_DIGIT_BASE = 2020


class DatabentoLocalFetcher(LoggingMixin):
    """
    Fetch historical futures data from a local Databento CSV file.

    Provides the same method interface as IBKRHistoricalFetcher for futures,
    allowing it to be used as a drop-in replacement in the accumulator.
    """

    def __init__(self, data_dir: str = "databento"):
        self.data_dir = Path(data_dir)
        self._data: Optional[List[Dict[str, Any]]] = None

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "DatabentoLocalFetcher":
        """Create from config dict with 'data_dir' key."""
        data_dir = config.get("data_dir", "databento")
        return cls(data_dir=data_dir)

    def _find_csv(self) -> Optional[Path]:
        """Find the Databento OHLCV CSV in the data directory."""
        if not self.data_dir.exists():
            return None
        csv_files = list(self.data_dir.glob("*.ohlcv-1d.csv"))
        if not csv_files:
            return None
        return max(csv_files, key=lambda p: p.stat().st_size)

    def _load_data(self) -> List[Dict[str, Any]]:
        """Load and parse the Databento CSV, filtering out spreads."""
        if self._data is not None:
            return self._data

        csv_path = self._find_csv()
        if csv_path is None:
            self.log(f"[X] No Databento CSV found in {self.data_dir}")
            self._data = []
            return self._data

        self.log(f"[*] Loading Databento CSV: {csv_path}")

        rows = []
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row["symbol"]

                # Filter out spread symbols (contain "-")
                if "-" in symbol:
                    continue

                parsed = self._parse_symbol(symbol)
                if parsed is None:
                    continue

                base_symbol, year, month = parsed

                date_str = row["ts_event"][:10]
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")

                close_price = float(row["close"])
                expiry_date = get_last_friday_of_month(year, month)
                expiry_yyyymm = f"{year:04d}{month:02d}"

                rows.append({
                    "date": date_obj,
                    "futures_price": close_price,
                    "expiry": expiry_date,
                    "expiry_yyyymm": expiry_yyyymm,
                    "symbol": symbol,
                    "base_symbol": base_symbol,
                    "volume": int(row["volume"]),
                })

        self._data = rows
        self.log(f"[OK] Loaded {len(rows)} rows from Databento CSV")
        return self._data

    @staticmethod
    def _parse_symbol(symbol: str) -> Optional[Tuple[str, int, int]]:
        """
        Parse Databento symbol like 'MBTG6' into (base_symbol, year, month).

        Returns:
            Tuple of (base_symbol, year, month) or None if invalid.
        """
        if len(symbol) < 4:
            return None

        year_digit = symbol[-1]
        month_code = symbol[-2]
        base = symbol[:-2]

        if not year_digit.isdigit():
            return None
        if month_code not in CME_MONTH_CODES:
            return None

        month = CME_MONTH_CODES[month_code]
        year = YEAR_DIGIT_BASE + int(year_digit)

        return (base, year, month)

    @staticmethod
    def expiry_to_databento_suffix(expiry_yyyymm: str) -> str:
        """
        Convert YYYYMM expiry to Databento symbol suffix.

        E.g., "202602" -> "G6"
        """
        year = int(expiry_yyyymm[:4])
        month = int(expiry_yyyymm[4:6])
        month_code = MONTH_TO_CME_CODE[month]
        year_digit = year % 10
        return f"{month_code}{year_digit}"

    def get_historical_futures(
        self,
        expiry: str = None,
        symbol: str = "MBT",
        exchange: str = "CME",
        start_date: datetime = None,
        end_date: datetime = None,
        bar_size: str = "1 day",
    ) -> List[Dict[str, Any]]:
        """
        Get historical futures prices for a specific contract expiry.

        Args:
            expiry: Contract expiry in YYYYMM format
            symbol: Base symbol (e.g., 'MBT')
            exchange: Ignored (data is always CME)
            start_date: Start date filter
            end_date: End date filter
            bar_size: Ignored (only daily data available)

        Returns:
            List of dicts with keys: date, futures_price, expiry
        """
        data = self._load_data()
        if not data:
            return []

        if expiry is None:
            expiry = get_front_month_expiry_str()

        target_suffix = self.expiry_to_databento_suffix(expiry)
        target_symbol = f"{symbol}{target_suffix}"

        self.log(f"[*] Databento: filtering for {target_symbol} (expiry {expiry})")

        result = []
        for row in data:
            if row["symbol"] != target_symbol:
                continue
            if start_date and row["date"] < start_date:
                continue
            if end_date and row["date"] > end_date:
                continue

            result.append({
                "date": row["date"],
                "futures_price": row["futures_price"],
                "expiry": row["expiry"],
            })

        result.sort(key=lambda x: x["date"])
        self.log(f"[OK] Databento: {len(result)} bars for {target_symbol}")
        return result

    def get_historical_continuous_futures(
        self,
        symbol: str = "MBT",
        exchange: str = "CME",
        start_date: datetime = None,
        end_date: datetime = None,
        bar_size: str = "1 day",
    ) -> List[Dict[str, Any]]:
        """
        Build a continuous futures series by rolling front-month contracts.

        For each trading date, selects the front-month contract's close price.

        Args:
            symbol: Base symbol (e.g., 'MBT')
            exchange: Ignored
            start_date: Start date filter
            end_date: End date filter
            bar_size: Ignored

        Returns:
            List of dicts with keys: date, futures_price
        """
        data = self._load_data()
        if not data:
            return []

        if start_date is None:
            start_date = datetime(2021, 5, 1)
        if end_date is None:
            end_date = datetime.now()

        filtered = [
            row for row in data
            if row["base_symbol"] == symbol
            and start_date <= row["date"] <= end_date
        ]

        if not filtered:
            self.log(f"[X] No data for {symbol} in {start_date.date()}-{end_date.date()}")
            return []

        expiry_schedule = generate_expiry_schedule(start_date, end_date)

        by_date = defaultdict(list)
        for row in filtered:
            by_date[row["date"].date()].append(row)

        result = []
        for date_key in sorted(by_date.keys()):
            date_obj = datetime.combine(date_key, datetime.min.time())
            front_expiry = get_front_month_expiry(date_obj, expiry_schedule)
            front_yyyymm = f"{front_expiry.year:04d}{front_expiry.month:02d}"

            bars_on_date = by_date[date_key]
            front_bar = None
            for bar in bars_on_date:
                if bar["expiry_yyyymm"] == front_yyyymm:
                    front_bar = bar
                    break

            if front_bar is not None:
                result.append({
                    "date": date_obj,
                    "futures_price": front_bar["futures_price"],
                })

        self.log(f"[OK] Databento continuous: {len(result)} bars for {symbol}")
        return result
