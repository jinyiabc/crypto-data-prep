#!/usr/bin/env python3
"""Accumulate and export futures + spot price data over a date range."""

import csv
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from crypto_data.data.ibkr import IBKRHistoricalFetcher
from crypto_data.data.databento import MONTH_TO_CME_CODE
from crypto_data.utils.expiry import (
    get_last_friday_of_month,
    get_front_month_expiry_str,
    generate_expiry_schedule,
    get_front_month_expiry,
)
from crypto_data.utils.logging import LoggingMixin


def format_contract_name(symbol: str, expiry_yyyymm: str) -> str:
    """Convert symbol + expiry to CME contract name. E.g., ('MBT', '202402') -> 'MBTG4'."""
    month = int(expiry_yyyymm[4:6])
    year_digit = int(expiry_yyyymm[:4]) % 10
    return f"{symbol}{MONTH_TO_CME_CODE[month]}{year_digit}"


CSV_FIELDNAMES = [
    "date",
    "contract",
    "spot_price",
    "futures_price",
    "future_continuous",
    "futures_expiry",
    "basis_absolute",
    "basis_percent",
    "monthly_basis",
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

    def _fetch_ibkr_spot_history(
        self,
        start_date: datetime,
        end_date: datetime,
        bar_size: str = "1 day",
        spot_config: Dict[str, str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical spot prices from IBKR using Crypto contract.

        Automatically chunks requests > 365 days into yearly segments.

        Args:
            start_date: Start date
            end_date: End date
            bar_size: Bar size ('1 day', '1 hour', etc.)
            spot_config: Dict with 'symbol', 'exchange', 'currency' keys.
                         Defaults to BTC on PAXOS in USD.

        Returns:
            List of dicts with date and spot_price
        """
        from ib_insync import Crypto

        if spot_config is None:
            spot_config = {"symbol": "BTC", "exchange": "PAXOS", "currency": "USD"}

        try:
            contract = Crypto(spot_config["symbol"], spot_config["exchange"], spot_config["currency"])
            self.fetcher.ib.qualifyContracts(contract)

            self.log(f"    Fetching {spot_config['symbol']}.{spot_config['currency']} spot from {spot_config['exchange']}...")

            max_days = 365
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
                    self.log(f"    Chunk {i + 1}/{len(chunks)}: {c_start.date()} to {c_end.date()} ({duration_days}d)")

                bars = self.fetcher.ib.reqHistoricalData(
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

                    date_key = date_obj.date()
                    if date_key not in seen_dates:
                        seen_dates.add(date_key)
                        result.append({
                            "date": date_obj,
                            "spot_price": bar.close,
                        })

                if i < len(chunks) - 1:
                    time.sleep(1)

            result.sort(key=lambda x: x["date"])
            self.log(f"[OK] Fetched {len(result)} spot bars from IBKR ({spot_config['symbol']}.{spot_config['currency']} {spot_config['exchange']})")
            return result

        except Exception as e:
            self.log(f"[X] Failed to fetch IBKR spot history: {e}")
            return []

    def _fetch_spot(
        self,
        start_date: datetime,
        end_date: datetime,
        spot_source: str = "ibkr",
        spot_symbol: str = "BTCUSDT",
        bar_size: str = "1 day",
        spot_config: Dict[str, str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch spot data from the configured source."""
        if spot_source == "ibkr":
            return self._fetch_ibkr_spot_history(start_date, end_date, bar_size, spot_config=spot_config)
        else:
            return self._fetch_binance_spot_history(start_date, end_date, spot_symbol)

    def _get_futures_fetcher(self, futures_source: str, databento_dir: str = None):
        """Get the appropriate futures fetcher based on source."""
        if futures_source == "databento":
            from crypto_data.data.databento import DatabentoLocalFetcher
            return DatabentoLocalFetcher(data_dir=databento_dir or "databento")
        return self.fetcher

    def accumulate(
        self,
        start_date: datetime,
        end_date: datetime,
        expiry: str = None,
        symbol: str = "MBT",
        exchange: str = "CME",
        spot_source: str = "ibkr",
        spot_symbol: str = "BTCUSDT",
        bar_size: str = "1 day",
        spot_config: Dict[str, str] = None,
        futures_source: str = "databento",
        databento_dir: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Accumulate futures and spot price data between start and end date.

        Fetches historical futures and spot prices, merges by date,
        and computes basis calculations.

        Args:
            start_date: Start date for historical data
            end_date: End date for historical data
            expiry: Futures contract expiry (YYYYMM), None = front-month
            symbol: Futures symbol ('MBT' or 'BTC')
            exchange: Futures exchange (default: 'CME')
            spot_source: Spot price source ('binance' or 'ibkr')
            spot_symbol: Binance spot pair (default: 'BTCUSDT'), ignored when spot_source='ibkr'
            bar_size: IBKR bar size ('1 day', '1 hour', etc.)
            spot_config: Dict with 'symbol', 'exchange', 'currency' for IBKR spot contract.
                         Defaults to BTC on PAXOS in USD.
            futures_source: Futures data source ('databento' or 'ibkr')
            databento_dir: Path to Databento data directory

        Returns:
            List of dicts sorted by date, each containing:
            date, spot_price, futures_price, futures_expiry,
            basis_absolute, basis_percent, annualized_basis, days_to_expiry
        """
        needs_ibkr = (futures_source == "ibkr") or (spot_source == "ibkr")
        if needs_ibkr and not self.fetcher.connected:
            if not self.fetcher.connect():
                return []

        if expiry is None:
            expiry = get_front_month_expiry_str()

        futures_label = "Databento" if futures_source == "databento" else f"IBKR ({exchange})"
        if spot_source == "ibkr" and spot_config:
            spot_label = f"IBKR {spot_config['symbol']}.{spot_config['currency']} {spot_config['exchange']}"
        elif spot_source == "ibkr":
            spot_label = "IBKR BTC.USD PAXOS"
        else:
            spot_label = f"Binance {spot_symbol}"
        contract_name = format_contract_name(symbol, expiry)
        self.log(f"[*] Accumulating futures data: {start_date.date()} to {end_date.date()}")
        self.log(f"    Contract: {contract_name}, Futures: {futures_label}, Spot: {spot_label}")

        spot_data = self._fetch_spot(
            start_date=start_date,
            end_date=end_date,
            spot_source=spot_source,
            spot_symbol=spot_symbol,
            bar_size=bar_size,
            spot_config=spot_config,
        )

        if not spot_data:
            self.log("[X] Failed to get spot data")
            return []

        fut_fetcher = self._get_futures_fetcher(futures_source, databento_dir)
        futures_data = fut_fetcher.get_historical_futures(
            expiry=expiry,
            symbol=symbol,
            exchange=exchange,
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

            monthly_basis = (
                basis_percent * (30 / days_to_expiry) if days_to_expiry > 0 else 0
            )
            annualized_basis = (
                basis_percent * (365 / days_to_expiry) if days_to_expiry > 0 else 0
            )

            result.append({
                "date": spot_entry["date"],
                "contract": contract_name,
                "spot_price": spot_price,
                "futures_price": futures_price,
                "futures_expiry": futures_expiry,
                "basis_absolute": basis_absolute,
                "basis_percent": basis_percent,
                "monthly_basis": monthly_basis,
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
        exchange: str = "CME",
        spot_source: str = "ibkr",
        spot_symbol: str = "BTCUSDT",
        bar_size: str = "1 day",
        spot_config: Dict[str, str] = None,
        futures_source: str = "databento",
        databento_dir: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Accumulate futures data with both front-month contract and continuous futures.

        Uses the front-month contract at start_date for futures_price and basis
        calculations, and continuous futures for the future_continuous column.

        Args:
            start_date: Start date for historical data
            end_date: End date for historical data
            symbol: Futures symbol ('MBT' or 'BTC')
            exchange: Futures exchange (default: 'CME')
            spot_source: Spot price source ('binance' or 'ibkr')
            spot_symbol: Binance spot pair (default: 'BTCUSDT'), ignored when spot_source='ibkr'
            bar_size: IBKR bar size ('1 day', '1 hour', etc.)
            spot_config: Dict with 'symbol', 'exchange', 'currency' for IBKR spot contract.
                         Defaults to BTC on PAXOS in USD.
            futures_source: Futures data source ('databento' or 'ibkr')
            databento_dir: Path to Databento data directory

        Returns:
            List of dicts sorted by date, each containing:
            date, spot_price, futures_price, future_continuous, futures_expiry,
            basis_absolute, basis_percent, annualized_basis, days_to_expiry
        """
        needs_ibkr = (futures_source == "ibkr") or (spot_source == "ibkr")
        if needs_ibkr and not self.fetcher.connected:
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

        futures_label = "Databento" if futures_source == "databento" else f"IBKR ({exchange})"
        if spot_source == "ibkr" and spot_config:
            spot_label = f"IBKR {spot_config['symbol']}.{spot_config['currency']} {spot_config['exchange']}"
        elif spot_source == "ibkr":
            spot_label = "IBKR BTC.USD PAXOS"
        else:
            spot_label = f"Binance {spot_symbol}"
        self.log(f"[*] Accumulating continuous futures: {start_date.date()} to {end_date.date()}")
        self.log(f"    Symbol: {symbol}, Futures: {futures_label}, Spot: {spot_label}")

        # Fetch spot data
        spot_data = self._fetch_spot(
            start_date=start_date,
            end_date=end_date,
            spot_source=spot_source,
            spot_symbol=spot_symbol,
            bar_size=bar_size,
            spot_config=spot_config,
        )

        if not spot_data:
            self.log("[X] Failed to get spot data")
            return []

        # Fetch front-month contract, fall back to next if expired/unavailable
        fut_fetcher = self._get_futures_fetcher(futures_source, databento_dir)
        futures_data = []
        expiry_str = None
        for candidate in candidates:
            expiry_str = f"{candidate.year:04d}{candidate.month:02d}"
            contract_name = format_contract_name(symbol, expiry_str)
            self.log(f"[*] Trying contract {contract_name}...")
            futures_data = fut_fetcher.get_historical_futures(
                expiry=expiry_str,
                symbol=symbol,
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                bar_size=bar_size,
            )
            if futures_data:
                self.log(f"[OK] Using contract {contract_name}")
                break
            self.log(f"[!] No data for {expiry_str}, trying next...")

        if not futures_data:
            self.log("[X] Failed to get futures data for any contract")
            return []

        futures_by_date = {}
        for entry in futures_data:
            futures_by_date[entry["date"].date()] = entry

        expiry_date = get_last_friday_of_month(int(expiry_str[:4]), int(expiry_str[4:6]))

        # Fetch continuous futures
        cont_data = fut_fetcher.get_historical_continuous_futures(
            symbol=symbol,
            exchange=exchange,
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

            monthly_basis = (
                basis_percent * (30 / days_to_expiry) if days_to_expiry > 0 else 0
            )
            annualized_basis = (
                basis_percent * (365 / days_to_expiry) if days_to_expiry > 0 else 0
            )

            result.append({
                "date": spot_entry["date"],
                "contract": format_contract_name(symbol, expiry_str),
                "spot_price": spot_price,
                "futures_price": futures_price,
                "future_continuous": cont_by_date.get(date_key),
                "futures_expiry": futures_expiry,
                "basis_absolute": basis_absolute,
                "basis_percent": basis_percent,
                "monthly_basis": monthly_basis,
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
                    "contract": row.get("contract", ""),
                    "spot_price": f"{row['spot_price']:.2f}",
                    "futures_price": f"{row['futures_price']:.2f}",
                    "future_continuous": f"{cont_price:.2f}" if cont_price is not None else "",
                    "futures_expiry": row["futures_expiry"].strftime("%Y-%m-%d")
                    if isinstance(row["futures_expiry"], datetime)
                    else row["futures_expiry"],
                    "basis_absolute": f"{row['basis_absolute']:.2f}",
                    "basis_percent": f"{row['basis_percent']:.2f}",
                    "monthly_basis": f"{row['monthly_basis']:.2f}",
                    "annualized_basis": f"{row['annualized_basis']:.2f}",
                    "days_to_expiry": row["days_to_expiry"],
                })

        self.log(f"[OK] Saved {len(data)} rows to {output_file}")
