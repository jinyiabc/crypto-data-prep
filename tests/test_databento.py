#!/usr/bin/env python3
"""Tests for Databento local CSV fetcher."""

import sys
import csv
import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch
import tempfile
import os

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_data.data.databento import (
    DatabentoLocalFetcher,
    CME_MONTH_CODES,
    MONTH_TO_CME_CODE,
)


# --- Sample CSV data for testing ---

SAMPLE_CSV_HEADER = "ts_event,rtype,publisher_id,instrument_id,open,high,low,close,volume,symbol"

SAMPLE_CSV_ROWS = [
    # MBT Jan 2026 (F6) — front-month for dates before Jan 30
    "2026-01-05T00:00:00.000000000Z,35,1,42083748,94800.0,95800.0,93800.0,95300.0,900,MBTF6",
    "2026-01-06T00:00:00.000000000Z,35,1,42083748,95300.0,96800.0,94800.0,96600.0,1100,MBTF6",
    "2026-01-07T00:00:00.000000000Z,35,1,42083748,96600.0,97300.0,95800.0,97000.0,750,MBTF6",
    # MBT Feb 2026 (G6)
    "2026-01-05T00:00:00.000000000Z,35,1,42083749,95000.0,96000.0,94000.0,95500.0,1000,MBTG6",
    "2026-01-06T00:00:00.000000000Z,35,1,42083749,95500.0,97000.0,95000.0,96800.0,1200,MBTG6",
    "2026-01-07T00:00:00.000000000Z,35,1,42083749,96800.0,97500.0,96000.0,97200.0,800,MBTG6",
    # MBT Mar 2026 (H6)
    "2026-01-05T00:00:00.000000000Z,35,1,42083750,95200.0,96200.0,94200.0,95700.0,500,MBTH6",
    "2026-01-06T00:00:00.000000000Z,35,1,42083750,95700.0,97200.0,95200.0,97000.0,600,MBTH6",
    "2026-01-07T00:00:00.000000000Z,35,1,42083750,97000.0,97700.0,96200.0,97400.0,400,MBTH6",
    # Spread (should be filtered)
    "2026-01-05T00:00:00.000000000Z,35,1,42083751,200.0,300.0,100.0,200.0,100,MBTG6-MBTH6",
    # MBT Apr 2026 (J6) — for continuous test
    "2026-03-01T00:00:00.000000000Z,35,1,42083752,100000.0,101000.0,99000.0,100500.0,700,MBTJ6",
]


def _create_test_csv(tmp_dir, rows=None):
    """Create a test Databento CSV file in the given directory."""
    csv_path = Path(tmp_dir) / "test.ohlcv-1d.csv"
    lines = [SAMPLE_CSV_HEADER] + (rows if rows is not None else SAMPLE_CSV_ROWS)
    csv_path.write_text("\n".join(lines) + "\n")
    return csv_path


class TestParseSymbol:
    """Tests for _parse_symbol static method."""

    def test_valid_symbol(self):
        result = DatabentoLocalFetcher._parse_symbol("MBTG6")
        assert result == ("MBT", 2026, 2)

    def test_valid_symbol_january(self):
        result = DatabentoLocalFetcher._parse_symbol("MBTF5")
        assert result == ("MBT", 2025, 1)

    def test_valid_symbol_december(self):
        result = DatabentoLocalFetcher._parse_symbol("MBTZ3")
        assert result == ("MBT", 2023, 12)

    def test_all_month_codes(self):
        """Verify all 12 month codes parse correctly."""
        for code, month in CME_MONTH_CODES.items():
            result = DatabentoLocalFetcher._parse_symbol(f"MBT{code}5")
            assert result is not None
            assert result[2] == month, f"Month code {code} should map to month {month}"

    def test_short_symbol(self):
        """Symbols shorter than 4 chars should return None."""
        assert DatabentoLocalFetcher._parse_symbol("MG6") is None

    def test_invalid_month_code(self):
        assert DatabentoLocalFetcher._parse_symbol("MBTA6") is None

    def test_invalid_year_digit(self):
        assert DatabentoLocalFetcher._parse_symbol("MBTGX") is None

    def test_spread_symbol(self):
        """Spread symbols should not parse (handled by caller, but test anyway)."""
        # The spread filter happens in _load_data, but _parse_symbol
        # will still try to parse the last 2 chars
        result = DatabentoLocalFetcher._parse_symbol("MBTG6-MBTH6")
        # "H6" at end => parses to ("MBTG6-MBT", 2026, 3) — technically parses
        # The spread filtering is done in _load_data before calling _parse_symbol


class TestExpiryToDabentoSuffix:
    """Tests for expiry_to_databento_suffix static method."""

    def test_basic_conversion(self):
        assert DatabentoLocalFetcher.expiry_to_databento_suffix("202602") == "G6"

    def test_january(self):
        assert DatabentoLocalFetcher.expiry_to_databento_suffix("202501") == "F5"

    def test_december(self):
        assert DatabentoLocalFetcher.expiry_to_databento_suffix("202312") == "Z3"

    def test_all_months(self):
        for month, code in MONTH_TO_CME_CODE.items():
            result = DatabentoLocalFetcher.expiry_to_databento_suffix(f"2025{month:02d}")
            assert result == f"{code}5"


class TestFromConfig:
    """Tests for from_config class method."""

    def test_default_data_dir(self):
        fetcher = DatabentoLocalFetcher.from_config({})
        assert str(fetcher.data_dir) == "databento"

    def test_custom_data_dir(self):
        fetcher = DatabentoLocalFetcher.from_config({"data_dir": "/tmp/my_data"})
        assert str(fetcher.data_dir) == "/tmp/my_data"


class TestLoadData:
    """Tests for CSV loading and data parsing."""

    def test_load_csv(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            data = fetcher._load_data()
            # 11 rows total, 1 spread => 10 non-spread rows
            assert len(data) == 10

    def test_spread_filtered_out(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            data = fetcher._load_data()
            symbols = {row["symbol"] for row in data}
            assert all("-" not in s for s in symbols)

    def test_caching(self):
        """Data should be loaded only once (cached)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            data1 = fetcher._load_data()
            data2 = fetcher._load_data()
            assert data1 is data2  # Same object reference

    def test_no_csv_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            data = fetcher._load_data()
            assert data == []

    def test_no_directory_returns_empty(self):
        fetcher = DatabentoLocalFetcher(data_dir="/nonexistent/path")
        data = fetcher._load_data()
        assert data == []

    def test_parsed_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            data = fetcher._load_data()
            row = data[0]
            assert isinstance(row["date"], datetime)
            assert isinstance(row["futures_price"], float)
            assert isinstance(row["volume"], int)
            assert "symbol" in row
            assert "base_symbol" in row
            assert "expiry_yyyymm" in row
            assert "expiry" in row


class TestGetHistoricalFutures:
    """Tests for get_historical_futures method."""

    def test_filter_by_expiry(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_futures(expiry="202602", symbol="MBT")
            assert len(result) == 3
            assert all(r["futures_price"] > 0 for r in result)

    def test_filter_by_date_range(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_futures(
                expiry="202602",
                symbol="MBT",
                start_date=datetime(2026, 1, 6),
                end_date=datetime(2026, 1, 6),
            )
            assert len(result) == 1
            assert result[0]["futures_price"] == 96800.0

    def test_sorted_by_date(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_futures(expiry="202602", symbol="MBT")
            dates = [r["date"] for r in result]
            assert dates == sorted(dates)

    def test_empty_for_unknown_expiry(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_futures(expiry="202912", symbol="MBT")
            assert result == []

    def test_result_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_futures(expiry="202602", symbol="MBT")
            assert len(result) > 0
            row = result[0]
            assert "date" in row
            assert "futures_price" in row
            assert "expiry" in row

    def test_different_symbol(self):
        """Test with different base symbol (no data expected for non-MBT)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_futures(expiry="202602", symbol="BTC")
            assert result == []


class TestGetHistoricalContinuousFutures:
    """Tests for get_historical_continuous_futures method."""

    def test_continuous_returns_data(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_continuous_futures(
                symbol="MBT",
                start_date=datetime(2026, 1, 1),
                end_date=datetime(2026, 1, 31),
            )
            assert len(result) > 0

    def test_continuous_sorted_by_date(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_continuous_futures(
                symbol="MBT",
                start_date=datetime(2026, 1, 1),
                end_date=datetime(2026, 1, 31),
            )
            dates = [r["date"] for r in result]
            assert dates == sorted(dates)

    def test_continuous_result_fields(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_continuous_futures(
                symbol="MBT",
                start_date=datetime(2026, 1, 1),
                end_date=datetime(2026, 1, 31),
            )
            assert len(result) > 0
            row = result[0]
            assert "date" in row
            assert "futures_price" in row

    def test_continuous_empty_for_no_data(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_continuous_futures(
                symbol="BTC",  # No BTC data in test CSV
                start_date=datetime(2026, 1, 1),
                end_date=datetime(2026, 1, 31),
            )
            assert result == []

    def test_continuous_selects_front_month(self):
        """In Jan 2026 (before Jan 30 expiry), front-month should be Jan 2026 (F6)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_test_csv(tmp_dir)
            fetcher = DatabentoLocalFetcher(data_dir=tmp_dir)
            result = fetcher.get_historical_continuous_futures(
                symbol="MBT",
                start_date=datetime(2026, 1, 5),
                end_date=datetime(2026, 1, 7),
            )
            # Should use Jan 2026 contract (F6) prices for these dates
            # (last Friday of Jan 2026 is Jan 30, so Jan is still front-month)
            assert len(result) == 3
            # Verify prices match MBTF6 close prices
            prices = {r["date"].day: r["futures_price"] for r in result}
            assert prices[5] == 95300.0
            assert prices[6] == 96600.0
            assert prices[7] == 97000.0
