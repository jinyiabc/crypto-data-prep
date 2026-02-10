#!/usr/bin/env python3
"""Tests for fetch-historical command."""

import sys
import csv
import pytest
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_data.data.ibkr import IBKRHistoricalFetcher
from crypto_data.data.accumulator import FuturesAccumulator
from crypto_data.utils.expiry import get_front_month_expiry_str, get_last_friday_of_month


class TestGetFrontMonthExpiry:
    """Tests for front-month expiry calculation."""

    def test_before_expiry_uses_current_month(self):
        """If today is before last Friday, use current month."""
        # First day of month - should use current month
        test_date = datetime(2026, 2, 1)
        result = get_front_month_expiry_str(test_date)
        assert result == "202602"

    def test_after_expiry_rolls_to_next_month(self):
        """If today is after last Friday, roll to next month."""
        # Last Friday of Feb 2026 is Feb 27
        test_date = datetime(2026, 2, 28)
        result = get_front_month_expiry_str(test_date)
        assert result == "202603"

    def test_december_rolls_to_january(self):
        """December should roll to January of next year."""
        test_date = datetime(2026, 12, 28)
        result = get_front_month_expiry_str(test_date)
        assert result == "202701"


class TestGetLastFridayOfMonth:
    """Tests for last Friday calculation."""

    def test_february_2026(self):
        """Last Friday of February 2026 is Feb 27."""
        result = get_last_friday_of_month(2026, 2)
        assert result.day == 27
        assert result.weekday() == 4  # Friday

    def test_march_2026(self):
        """Last Friday of March 2026 is Mar 27."""
        result = get_last_friday_of_month(2026, 3)
        assert result.day == 27
        assert result.weekday() == 4

    def test_december_2026(self):
        """Last Friday of December 2026 is Dec 25."""
        result = get_last_friday_of_month(2026, 12)
        assert result.day == 25
        assert result.weekday() == 4


class TestIBKRHistoricalFetcher:
    """Tests for IBKRHistoricalFetcher."""

    def test_from_config(self):
        """Test creating fetcher from config dict."""
        config = {
            "host": "127.0.0.1",
            "port": 7496,
            "client_id": 2,
            "timeout": 15,
        }
        fetcher = IBKRHistoricalFetcher.from_config(config)

        assert fetcher.host == "127.0.0.1"
        assert fetcher.port == 7496
        assert fetcher.client_id == 2
        assert fetcher.timeout == 15

    def test_from_config_defaults(self):
        """Test creating fetcher with minimal config."""
        config = {}
        fetcher = IBKRHistoricalFetcher.from_config(config)

        assert fetcher.host == "127.0.0.1"
        assert fetcher.port is None
        assert fetcher.client_id == 1
        assert fetcher.timeout == 10

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.connect")
    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_spot")
    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.disconnect")
    def test_fetch_historical_workflow(self, mock_disconnect, mock_get_hist, mock_connect):
        """Test the fetch historical workflow with mocks."""
        mock_connect.return_value = True
        mock_get_hist.return_value = [
            {"date": datetime(2026, 1, 1), "etf_price": 50.0, "btc_price": 92500.0},
            {"date": datetime(2026, 1, 2), "etf_price": 51.0, "btc_price": 94350.0},
        ]

        fetcher = IBKRHistoricalFetcher()

        assert fetcher.connect() is True
        data = fetcher.get_historical_spot(symbol="IBIT")
        fetcher.disconnect()

        assert len(data) == 2
        assert data[0]["etf_price"] == 50.0
        assert data[1]["btc_price"] == 94350.0
        mock_disconnect.assert_called_once()


class TestFuturesAccumulator:
    """Tests for FuturesAccumulator."""

    def _make_accumulator(self):
        fetcher = IBKRHistoricalFetcher()
        fetcher.connected = True
        return FuturesAccumulator(fetcher)

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_futures")
    @patch("crypto_data.data.accumulator.FuturesAccumulator._fetch_binance_spot_history")
    def test_returns_merged_data_with_basis(self, mock_spot, mock_futures):
        """Test that spot and futures are merged with basis calculations."""
        expiry_date = get_last_friday_of_month(2026, 3)
        mock_spot.return_value = [
            {"date": datetime(2026, 1, 15), "spot_price": 92500.0},
            {"date": datetime(2026, 1, 16), "spot_price": 94350.0},
        ]
        mock_futures.return_value = [
            {"date": datetime(2026, 1, 15), "futures_price": 93500.0, "expiry": expiry_date},
            {"date": datetime(2026, 1, 16), "futures_price": 95400.0, "expiry": expiry_date},
        ]

        acc = self._make_accumulator()
        result = acc.accumulate(
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 16),
            expiry="202603",
        )

        assert len(result) == 2
        row = result[0]
        assert row["date"] == datetime(2026, 1, 15)
        assert row["spot_price"] == 92500.0
        assert row["futures_price"] == 93500.0
        assert row["futures_expiry"] == expiry_date
        assert row["basis_absolute"] == pytest.approx(1000.0)
        assert row["basis_percent"] == pytest.approx((1000.0 / 92500.0) * 100)
        assert row["days_to_expiry"] == (expiry_date - datetime(2026, 1, 15)).days
        assert row["annualized_basis"] == pytest.approx(
            row["basis_percent"] * (365 / row["days_to_expiry"])
        )

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_futures")
    @patch("crypto_data.data.accumulator.FuturesAccumulator._fetch_binance_spot_history")
    def test_skips_dates_without_futures(self, mock_spot, mock_futures):
        """Dates present in spot but missing from futures are skipped."""
        expiry_date = get_last_friday_of_month(2026, 3)
        mock_spot.return_value = [
            {"date": datetime(2026, 1, 15), "spot_price": 92500.0},
            {"date": datetime(2026, 1, 16), "spot_price": 94350.0},
            {"date": datetime(2026, 1, 17), "spot_price": 96200.0},
        ]
        mock_futures.return_value = [
            {"date": datetime(2026, 1, 15), "futures_price": 93500.0, "expiry": expiry_date},
            # 1/16 missing
            {"date": datetime(2026, 1, 17), "futures_price": 97200.0, "expiry": expiry_date},
        ]

        acc = self._make_accumulator()
        result = acc.accumulate(
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 17),
            expiry="202603",
        )

        assert len(result) == 2
        assert result[0]["date"] == datetime(2026, 1, 15)
        assert result[1]["date"] == datetime(2026, 1, 17)

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_futures")
    @patch("crypto_data.data.accumulator.FuturesAccumulator._fetch_binance_spot_history")
    def test_returns_empty_when_spot_fails(self, mock_spot, mock_futures):
        """Returns empty list when Binance spot data fetch fails."""
        mock_spot.return_value = []

        acc = self._make_accumulator()
        result = acc.accumulate(
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 31),
            expiry="202603",
        )

        assert result == []
        mock_futures.assert_not_called()

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_futures")
    @patch("crypto_data.data.accumulator.FuturesAccumulator._fetch_binance_spot_history")
    def test_returns_empty_when_futures_fails(self, mock_spot, mock_futures):
        """Returns empty list when futures data fetch fails."""
        mock_spot.return_value = [
            {"date": datetime(2026, 1, 15), "spot_price": 92500.0},
        ]
        mock_futures.return_value = []

        acc = self._make_accumulator()
        result = acc.accumulate(
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 31),
            expiry="202603",
        )

        assert result == []

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_futures")
    @patch("crypto_data.data.accumulator.FuturesAccumulator._fetch_binance_spot_history")
    @patch("crypto_data.data.accumulator.get_front_month_expiry_str", return_value="202603")
    def test_defaults_to_front_month_expiry(self, mock_expiry, mock_spot, mock_futures):
        """Uses front-month expiry when none specified."""
        mock_spot.return_value = [
            {"date": datetime(2026, 1, 15), "spot_price": 92500.0},
        ]
        mock_futures.return_value = [
            {"date": datetime(2026, 1, 15), "futures_price": 93500.0, "expiry": None},
        ]

        acc = self._make_accumulator()
        acc.accumulate(
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 15),
        )

        mock_expiry.assert_called_once()
        mock_futures.assert_called_once()
        assert mock_futures.call_args.kwargs["expiry"] == "202603"

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.connect")
    def test_returns_empty_when_not_connected(self, mock_connect):
        """Returns empty list when connection fails."""
        mock_connect.return_value = False

        fetcher = IBKRHistoricalFetcher()
        acc = FuturesAccumulator(fetcher)
        result = acc.accumulate(
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 31),
        )

        assert result == []

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_futures")
    @patch("crypto_data.data.accumulator.FuturesAccumulator._fetch_binance_spot_history")
    def test_to_csv(self, mock_spot, mock_futures, tmp_path):
        """Test CSV export writes correct columns and values."""
        expiry_date = get_last_friday_of_month(2026, 3)
        mock_spot.return_value = [
            {"date": datetime(2026, 1, 15), "spot_price": 92500.0},
        ]
        mock_futures.return_value = [
            {"date": datetime(2026, 1, 15), "futures_price": 93500.0, "expiry": expiry_date},
        ]

        acc = self._make_accumulator()
        data = acc.accumulate(
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 15),
            expiry="202603",
        )

        output_file = tmp_path / "test_basis.csv"
        acc.to_csv(data, str(output_file))

        assert output_file.exists()
        with open(output_file, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            row = rows[0]
            assert row["date"] == "2026-01-15"
            assert float(row["spot_price"]) == 92500.0
            assert float(row["futures_price"]) == 93500.0
            assert row["futures_expiry"] == expiry_date.strftime("%Y-%m-%d")
            assert float(row["basis_absolute"]) == 1000.0
            assert int(row["days_to_expiry"]) == (expiry_date - datetime(2026, 1, 15)).days


class TestAccumulateContinuous:
    """Tests for FuturesAccumulator.accumulate_continuous."""

    def _make_accumulator(self):
        fetcher = IBKRHistoricalFetcher()
        fetcher.connected = True
        return FuturesAccumulator(fetcher)

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_continuous_futures")
    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_futures")
    @patch("crypto_data.data.accumulator.FuturesAccumulator._fetch_binance_spot_history")
    def test_uses_front_month_at_start_date(self, mock_spot, mock_futures, mock_cont):
        """Test that futures_price uses front-month contract at start_date."""
        # start_date Jan 20 -> front-month is Jan (last Friday = Jan 30)
        jan_expiry = get_last_friday_of_month(2026, 1)

        mock_spot.return_value = [
            {"date": datetime(2026, 1, 20), "spot_price": 90000.0},
            {"date": datetime(2026, 1, 27), "spot_price": 91000.0},
            {"date": datetime(2026, 2, 3), "spot_price": 92000.0},
        ]

        # Single contract (front-month at start_date = 202601)
        mock_futures.return_value = [
            {"date": datetime(2026, 1, 20), "futures_price": 91000.0, "expiry": jan_expiry},
            {"date": datetime(2026, 1, 27), "futures_price": 91500.0, "expiry": jan_expiry},
            {"date": datetime(2026, 2, 3), "futures_price": 92000.0, "expiry": jan_expiry},
        ]

        mock_cont.return_value = [
            {"date": datetime(2026, 1, 20), "futures_price": 91100.0},
            {"date": datetime(2026, 1, 27), "futures_price": 91600.0},
            {"date": datetime(2026, 2, 3), "futures_price": 93100.0},
        ]

        acc = self._make_accumulator()
        result = acc.accumulate_continuous(
            start_date=datetime(2026, 1, 20),
            end_date=datetime(2026, 2, 3),
        )

        assert len(result) == 3

        # All rows use the same contract (202601, expiry = jan_expiry)
        for row in result:
            assert row["futures_expiry"] == jan_expiry

        # futures_price comes from the single contract
        assert result[0]["futures_price"] == 91000.0
        assert result[1]["futures_price"] == 91500.0
        assert result[2]["futures_price"] == 92000.0

        # future_continuous comes from ContFuture
        assert result[0]["future_continuous"] == 91100.0
        assert result[1]["future_continuous"] == 91600.0
        assert result[2]["future_continuous"] == 93100.0

        # get_historical_futures called once with front-month expiry at start_date
        mock_futures.assert_called_once()
        assert mock_futures.call_args.kwargs["expiry"] == "202601"

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_futures")
    @patch("crypto_data.data.accumulator.FuturesAccumulator._fetch_binance_spot_history")
    def test_returns_empty_when_spot_fails(self, mock_spot, mock_futures):
        """Returns empty list when Binance spot fetch fails."""
        mock_spot.return_value = []

        acc = self._make_accumulator()
        result = acc.accumulate_continuous(
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 2, 28),
        )

        assert result == []
        mock_futures.assert_not_called()

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_futures")
    @patch("crypto_data.data.accumulator.FuturesAccumulator._fetch_binance_spot_history")
    def test_returns_empty_when_futures_fail(self, mock_spot, mock_futures):
        """Returns empty list when futures contract returns no data."""
        mock_spot.return_value = [
            {"date": datetime(2026, 1, 15), "spot_price": 90000.0},
        ]
        mock_futures.return_value = []

        acc = self._make_accumulator()
        result = acc.accumulate_continuous(
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 15),
        )

        assert result == []


class TestGetHistoricalContinuousFutures:
    """Tests for IBKRHistoricalFetcher.get_historical_continuous_futures."""

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.connect")
    def test_returns_empty_when_not_connected(self, mock_connect):
        """Returns empty list when connection fails."""
        mock_connect.return_value = False
        fetcher = IBKRHistoricalFetcher()
        result = fetcher.get_historical_continuous_futures(symbol="MBT")
        assert result == []

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher._get_ib")
    def test_returns_continuous_bars(self, mock_get_ib):
        """Test that continuous futures returns price data from ContFuture."""
        mock_ib = MagicMock()
        mock_get_ib.return_value = mock_ib

        # Mock bars returned by reqHistoricalData
        mock_bar1 = Mock()
        mock_bar1.date = datetime(2026, 1, 15)
        mock_bar1.close = 93500.0
        mock_bar2 = Mock()
        mock_bar2.date = datetime(2026, 1, 16)
        mock_bar2.close = 94000.0

        mock_ib.reqHistoricalData.return_value = [mock_bar1, mock_bar2]
        mock_ib.qualifyContracts.return_value = []

        fetcher = IBKRHistoricalFetcher()
        fetcher.connected = True
        fetcher.ib = mock_ib

        result = fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 16),
        )

        assert len(result) == 2
        assert result[0]["date"] == datetime(2026, 1, 15)
        assert result[0]["futures_price"] == 93500.0
        assert result[1]["date"] == datetime(2026, 1, 16)
        assert result[1]["futures_price"] == 94000.0

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher._get_ib")
    def test_handles_exception(self, mock_get_ib):
        """Test that exceptions are caught and return empty list."""
        mock_ib = MagicMock()
        mock_get_ib.return_value = mock_ib
        mock_ib.qualifyContracts.side_effect = Exception("No security definition")

        fetcher = IBKRHistoricalFetcher()
        fetcher.connected = True
        fetcher.ib = mock_ib

        result = fetcher.get_historical_continuous_futures(symbol="MBT")
        assert result == []


class TestFetchHistoricalCommand:
    """Integration tests for fetch-historical command."""

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.connect")
    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.get_historical_spot")
    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.disconnect")
    def test_fetch_historical_saves_csv(self, mock_disconnect, mock_get_hist, mock_connect, tmp_path):
        """Test that fetch-historical saves data to CSV."""
        mock_connect.return_value = True
        mock_get_hist.return_value = [
            {"date": datetime(2026, 1, 1), "etf_price": 50.0, "btc_price": 92500.0},
            {"date": datetime(2026, 1, 2), "etf_price": 51.0, "btc_price": 94350.0},
        ]

        fetcher = IBKRHistoricalFetcher()
        fetcher.connect()
        data = fetcher.get_historical_spot(symbol="IBIT")
        fetcher.disconnect()

        # Save to CSV
        output_file = tmp_path / "test_output.csv"
        with open(output_file, "w", newline="") as f:
            fieldnames = ["date", "etf_price", "btc_price"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow({
                    "date": row["date"].isoformat() if isinstance(row["date"], datetime) else row["date"],
                    "etf_price": row["etf_price"],
                    "btc_price": row["btc_price"],
                })

        # Verify CSV
        assert output_file.exists()
        with open(output_file, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2
            assert float(rows[0]["etf_price"]) == 50.0
            assert float(rows[1]["btc_price"]) == 94350.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
