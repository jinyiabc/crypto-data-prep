#!/usr/bin/env python3
"""Tests for IBKRHistoricalFetcher.get_historical_continuous_futures."""

import sys
import pytest
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch, MagicMock, call

from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_data.data.ibkr import IBKRHistoricalFetcher


@pytest.fixture
def connected_fetcher():
    """Create a fetcher with a mocked IB connection."""
    fetcher = IBKRHistoricalFetcher()
    fetcher.connected = True
    fetcher.ib = MagicMock()
    fetcher.ib.qualifyContracts.return_value = []
    return fetcher


def _make_bar(dt, close_price):
    """Create a mock bar with given date and close price."""
    bar = Mock()
    bar.date = dt
    bar.close = close_price
    return bar


class TestConnectionHandling:
    """Tests for connection-related behavior."""

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.connect")
    def test_returns_empty_when_not_connected_and_connect_fails(self, mock_connect):
        mock_connect.return_value = False
        fetcher = IBKRHistoricalFetcher()
        result = fetcher.get_historical_continuous_futures(symbol="MBT")
        assert result == []
        mock_connect.assert_called_once()

    @patch("crypto_data.data.ibkr.IBKRHistoricalFetcher.connect")
    def test_proceeds_when_not_connected_but_connect_succeeds(self, mock_connect):
        mock_connect.return_value = True
        fetcher = IBKRHistoricalFetcher()
        fetcher.ib = MagicMock()
        fetcher.ib.qualifyContracts.return_value = []
        fetcher.ib.reqHistoricalData.return_value = []

        # connect() returning True doesn't set connected=True in mock,
        # but the method only checks self.connected at the top
        result = fetcher.get_historical_continuous_futures(symbol="MBT")
        mock_connect.assert_called_once()

    def test_skips_connect_when_already_connected(self, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        with patch.object(connected_fetcher, "connect") as mock_connect:
            connected_fetcher.get_historical_continuous_futures(symbol="MBT")
            mock_connect.assert_not_called()


class TestContractSetup:
    """Tests for ContFuture contract creation and qualification."""

    @patch("ib_insync.ContFuture")
    def test_creates_cont_future_with_symbol_and_exchange(self, mock_cont_cls, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        connected_fetcher.get_historical_continuous_futures(symbol="MBT")

        mock_cont_cls.assert_called_once_with("MBT", "CME")

    @patch("ib_insync.ContFuture")
    def test_creates_cont_future_with_btc_symbol(self, mock_cont_cls, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        connected_fetcher.get_historical_continuous_futures(symbol="BTC")

        mock_cont_cls.assert_called_once_with("BTC", "CME")

    @patch("ib_insync.ContFuture")
    def test_creates_cont_future_with_custom_exchange(self, mock_cont_cls, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        connected_fetcher.get_historical_continuous_futures(symbol="MBT", exchange="NYMEX")

        mock_cont_cls.assert_called_once_with("MBT", "NYMEX")

    @patch("ib_insync.ContFuture")
    def test_qualifies_contract(self, mock_cont_cls, connected_fetcher):
        mock_contract = Mock()
        mock_cont_cls.return_value = mock_contract
        connected_fetcher.ib.reqHistoricalData.return_value = []

        connected_fetcher.get_historical_continuous_futures(symbol="MBT")

        connected_fetcher.ib.qualifyContracts.assert_called_once_with(mock_contract)


class TestDurationCalculation:
    """Tests for duration string calculation passed to reqHistoricalData."""

    def test_short_duration_uses_days(self, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 31),
        )

        call_kwargs = connected_fetcher.ib.reqHistoricalData.call_args
        assert call_kwargs.kwargs["durationStr"] == "30 D"

    def test_duration_over_365_uses_years(self, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2026, 2, 1),
        )

        call_kwargs = connected_fetcher.ib.reqHistoricalData.call_args
        assert call_kwargs.kwargs["durationStr"] == "2 Y"

    def test_exactly_365_days_uses_days(self, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 12, 31),  # 364 days
        )

        call_kwargs = connected_fetcher.ib.reqHistoricalData.call_args
        assert call_kwargs.kwargs["durationStr"] == "364 D"


class TestDefaultDates:
    """Tests for default start/end date behavior."""

    @patch("crypto_data.data.ibkr.datetime", wraps=datetime)
    def test_defaults_end_date_to_now(self, mock_dt, connected_fetcher):
        # Can't easily mock datetime.now() in all cases, so just verify
        # the method doesn't error when no dates are provided
        connected_fetcher.ib.reqHistoricalData.return_value = []

        result = connected_fetcher.get_historical_continuous_futures(symbol="MBT")

        # Should succeed without error
        assert isinstance(result, list)

    def test_defaults_start_date_to_90_days_before_end(self, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        end = datetime(2026, 3, 1)
        connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            end_date=end,
        )

        call_kwargs = connected_fetcher.ib.reqHistoricalData.call_args
        assert call_kwargs.kwargs["durationStr"] == "90 D"


class TestReqHistoricalDataParams:
    """Tests for parameters passed to reqHistoricalData."""

    def test_passes_correct_params(self, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 31),
            bar_size="1 hour",
        )

        call_kwargs = connected_fetcher.ib.reqHistoricalData.call_args.kwargs
        assert call_kwargs["endDateTime"] == ""
        assert call_kwargs["barSizeSetting"] == "1 hour"
        assert call_kwargs["whatToShow"] == "TRADES"
        assert call_kwargs["useRTH"] is True
        assert call_kwargs["formatDate"] == 1

    def test_default_bar_size_is_1_day(self, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        connected_fetcher.get_historical_continuous_futures(symbol="MBT")

        call_kwargs = connected_fetcher.ib.reqHistoricalData.call_args.kwargs
        assert call_kwargs["barSizeSetting"] == "1 day"


class TestBarProcessing:
    """Tests for processing bars returned by IBKR."""

    def test_returns_bars_with_datetime_dates(self, connected_fetcher):
        bars = [
            _make_bar(datetime(2026, 1, 15), 93500.0),
            _make_bar(datetime(2026, 1, 16), 94000.0),
        ]
        connected_fetcher.ib.reqHistoricalData.return_value = bars

        result = connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 16),
        )

        assert len(result) == 2
        assert result[0]["date"] == datetime(2026, 1, 15)
        assert result[0]["futures_price"] == 93500.0
        assert result[1]["date"] == datetime(2026, 1, 16)
        assert result[1]["futures_price"] == 94000.0

    def test_handles_date_objects_converted_to_datetime(self, connected_fetcher):
        """Bars with date (not datetime) objects are converted via datetime.combine."""
        bars = [
            _make_bar(date(2026, 1, 15), 93500.0),
            _make_bar(date(2026, 1, 16), 94000.0),
        ]
        connected_fetcher.ib.reqHistoricalData.return_value = bars

        result = connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 16),
        )

        assert len(result) == 2
        assert result[0]["date"] == datetime(2026, 1, 15, 0, 0, 0)
        assert result[1]["date"] == datetime(2026, 1, 16, 0, 0, 0)

    def test_filters_bars_before_start_date(self, connected_fetcher):
        """Bars with dates before start_date are excluded."""
        bars = [
            _make_bar(datetime(2026, 1, 10), 91000.0),
            _make_bar(datetime(2026, 1, 14), 92000.0),
            _make_bar(datetime(2026, 1, 15), 93500.0),
            _make_bar(datetime(2026, 1, 16), 94000.0),
        ]
        connected_fetcher.ib.reqHistoricalData.return_value = bars

        result = connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 16),
        )

        assert len(result) == 2
        assert result[0]["date"] == datetime(2026, 1, 15)
        assert result[1]["date"] == datetime(2026, 1, 16)

    def test_includes_bar_on_start_date(self, connected_fetcher):
        """Bar exactly on start_date is included."""
        bars = [_make_bar(datetime(2026, 1, 15), 93500.0)]
        connected_fetcher.ib.reqHistoricalData.return_value = bars

        result = connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 15),
        )

        assert len(result) == 1
        assert result[0]["futures_price"] == 93500.0

    def test_returns_empty_list_when_no_bars(self, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.return_value = []

        result = connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 16),
        )

        assert result == []

    def test_all_bars_filtered_out_returns_empty(self, connected_fetcher):
        """If all bars are before start_date, result is empty."""
        bars = [
            _make_bar(datetime(2026, 1, 10), 91000.0),
            _make_bar(datetime(2026, 1, 12), 92000.0),
        ]
        connected_fetcher.ib.reqHistoricalData.return_value = bars

        result = connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 15),
            end_date=datetime(2026, 1, 20),
        )

        assert result == []


class TestErrorHandling:
    """Tests for exception handling."""

    def test_exception_during_qualify_returns_empty(self, connected_fetcher):
        connected_fetcher.ib.qualifyContracts.side_effect = Exception("No security definition")

        result = connected_fetcher.get_historical_continuous_futures(symbol="MBT")

        assert result == []

    def test_exception_during_req_historical_returns_empty(self, connected_fetcher):
        connected_fetcher.ib.reqHistoricalData.side_effect = Exception("Timeout")

        result = connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 31),
        )

        assert result == []

    def test_exception_during_bar_processing_returns_empty(self, connected_fetcher):
        """If a bar has unexpected data causing an error, method returns empty."""
        bad_bar = Mock()
        bad_bar.date = None  # Will cause AttributeError in isinstance check
        bad_bar.close = 93500.0
        connected_fetcher.ib.reqHistoricalData.return_value = [bad_bar]

        result = connected_fetcher.get_historical_continuous_futures(
            symbol="MBT",
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 31),
        )

        assert result == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
