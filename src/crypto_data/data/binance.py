#!/usr/bin/env python3
"""
Binance data fetcher for BTC spot and futures prices.

Consolidated from fetch_futures_binance.py and get_btc_prices.py
"""

import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from crypto_data.data.base import BaseFetcher


class BinanceFetcher(BaseFetcher):
    """Fetch BTC spot and futures prices from Binance API."""

    SPOT_API = "https://api.binance.com/api/v3"
    FUTURES_API = "https://fapi.binance.com/fapi/v1"
    COIN_FUTURES_API = "https://dapi.binance.com/dapi/v1"

    def __init__(self, timeout: int = 10):
        super().__init__(timeout)

    def fetch_spot_price(self, symbol: str = "BTCUSDT") -> Optional[float]:
        """
        Fetch BTC spot price from Binance.

        Args:
            symbol: Trading pair (default: BTCUSDT)

        Returns:
            Spot price or None if fetch failed
        """
        try:
            url = f"{self.SPOT_API}/ticker/price"
            response = requests.get(
                url, params={"symbol": symbol}, timeout=self.timeout
            )
            response.raise_for_status()
            return float(response.json()["price"])
        except Exception as e:
            self.log_error(f"Error fetching Binance spot: {e}")
            return None

    def fetch_futures_price(self, expiry: str = None) -> Optional[Dict[str, Any]]:
        """
        Fetch BTC perpetual futures from Binance.

        Args:
            expiry: Not used for perpetual (None = perpetual)

        Returns:
            Dictionary with futures data or None
        """
        return self.fetch_perpetual_futures()

    def fetch_perpetual_futures(self, symbol: str = "BTCUSDT") -> Optional[Dict[str, Any]]:
        """
        Fetch perpetual futures data including funding rate.

        Args:
            symbol: Futures symbol (default: BTCUSDT)

        Returns:
            Dictionary with perpetual futures data
        """
        try:
            # Get spot price
            spot_url = f"{self.SPOT_API}/ticker/price"
            spot_response = requests.get(
                spot_url, params={"symbol": symbol}, timeout=self.timeout
            )
            spot_price = float(spot_response.json()["price"])

            # Get futures price
            futures_url = f"{self.FUTURES_API}/ticker/price"
            futures_response = requests.get(
                futures_url, params={"symbol": symbol}, timeout=self.timeout
            )
            futures_price = float(futures_response.json()["price"])

            # Get funding rate
            funding_url = f"{self.FUTURES_API}/premiumIndex"
            funding_response = requests.get(
                funding_url, params={"symbol": symbol}, timeout=self.timeout
            )
            funding_data = funding_response.json()
            funding_rate = float(funding_data["lastFundingRate"])

            basis_absolute = futures_price - spot_price
            basis_percent = basis_absolute / spot_price

            return {
                "exchange": "Binance",
                "type": "Perpetual",
                "symbol": symbol,
                "spot_price": spot_price,
                "futures_price": futures_price,
                "basis_absolute": basis_absolute,
                "basis_percent": basis_percent * 100,
                "funding_rate_8h": funding_rate * 100,  # % per 8 hours
                "funding_rate_annual": funding_rate * 3 * 365 * 100,  # Annualized
                "mark_price": float(funding_data.get("markPrice", futures_price)),
                "index_price": float(funding_data.get("indexPrice", spot_price)),
                "timestamp": datetime.now(),
                "note": "Perpetual - no expiry, uses funding rate",
            }

        except Exception as e:
            self.log_error(f"Error fetching Binance perpetual: {e}")
            return None

    def fetch_coin_futures(self, symbol: str = "BTCUSD_PERP") -> Optional[Dict[str, Any]]:
        """
        Fetch coin-margined perpetual futures (BTCUSD).

        Args:
            symbol: Coin futures symbol

        Returns:
            Dictionary with futures data
        """
        try:
            url = f"{self.COIN_FUTURES_API}/ticker/24hr"
            response = requests.get(
                url, params={"symbol": symbol}, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                data = data[0]

            # Get funding rate
            funding_url = f"{self.COIN_FUTURES_API}/fundingRate"
            funding_response = requests.get(
                funding_url, params={"symbol": symbol, "limit": 1}, timeout=self.timeout
            )
            funding_data = funding_response.json()
            funding_rate = float(funding_data[0]["fundingRate"]) if funding_data else 0

            mark_price = float(data["lastPrice"])
            index_price = float(data["indexPrice"])

            return {
                "symbol": symbol,
                "type": "perpetual",
                "mark_price": mark_price,
                "index_price": index_price,
                "funding_rate": funding_rate,
                "funding_rate_annual": funding_rate * 3 * 365 * 100,
                "volume_24h": float(data["volume"]),
                "open_interest": float(data["openInterest"]),
                "basis_absolute": mark_price - index_price,
                "basis_percent": (mark_price - index_price) / index_price,
            }

        except Exception as e:
            self.log_error(f"Error fetching Binance coin futures: {e}")
            return None

    def _get_quarterly_symbol(self, expiry: str) -> str:
        """
        Convert YYYYMM expiry to Binance quarterly symbol (e.g., BTCUSD_250627).

        Args:
            expiry: Expiry in YYYYMM format (e.g., '202506')

        Returns:
            Binance symbol like 'BTCUSD_250627'
        """
        from crypto_data.utils.expiry import get_last_friday_of_month

        year = int(expiry[:4])
        month = int(expiry[4:6])
        last_friday = get_last_friday_of_month(year, month)
        return f"BTCUSD_{last_friday.strftime('%y%m%d')}"

    def list_available_contracts(self) -> List[str]:
        """List available BTCUSD quarterly futures contracts on Binance."""
        try:
            url = f"{self.COIN_FUTURES_API}/exchangeInfo"
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return [
                s["symbol"] for s in data["symbols"]
                if s["symbol"].startswith("BTCUSD") and s["symbol"] != "BTCUSD_PERP"
            ]
        except Exception:
            return []

    def get_historical_futures_klines(
        self,
        expiry: str,
        days: int = 90,
        interval: str = "1d",
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical klines (candlesticks) for a quarterly futures contract.

        Uses Binance coin-margined futures API (dapi).

        Args:
            expiry: Contract expiry in YYYYMM format (e.g., '202506')
            days: Number of days of history to fetch
            interval: Kline interval (1m, 5m, 1h, 1d, etc.)

        Returns:
            List of dicts with date, open, high, low, close, volume, futures_price, expiry
        """
        symbol = self._get_quarterly_symbol(expiry)

        end_ms = int(datetime.now().timestamp() * 1000)
        start_ms = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

        all_klines = []
        current_start = start_ms

        try:
            while current_start < end_ms:
                url = f"{self.COIN_FUTURES_API}/klines"
                params = {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": current_start,
                    "endTime": end_ms,
                    "limit": 1500,
                }
                response = requests.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                klines = response.json()

                if not klines:
                    break

                for k in klines:
                    open_time = datetime.fromtimestamp(k[0] / 1000)
                    all_klines.append({
                        "date": open_time,
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume": float(k[5]),
                        "futures_price": float(k[4]),
                        "expiry": expiry,
                    })

                # Move past the last kline
                current_start = int(klines[-1][6]) + 1  # closeTime + 1ms

                if len(klines) < 1500:
                    break

            return all_klines

        except Exception as e:
            self.log_error(f"Error fetching historical klines for {symbol}: {e}")
            return []

    def fetch_quarterly_futures(self) -> List[Dict[str, Any]]:
        """
        Fetch all quarterly futures contracts (CME-style).

        Returns:
            List of quarterly contract data sorted by expiry
        """
        try:
            # Get available contracts
            url = f"{self.COIN_FUTURES_API}/exchangeInfo"
            response = requests.get(url, timeout=self.timeout)
            data = response.json()

            quarterly_contracts = []

            for symbol_info in data["symbols"]:
                symbol = symbol_info["symbol"]

                # Only quarterly contracts (e.g., BTCUSD_241227)
                if symbol.startswith("BTCUSD_") and symbol != "BTCUSD_PERP":
                    # Get ticker
                    ticker_url = f"{self.COIN_FUTURES_API}/ticker/24hr"
                    ticker_response = requests.get(
                        ticker_url, params={"symbol": symbol}, timeout=self.timeout
                    )
                    ticker_data = ticker_response.json()

                    if isinstance(ticker_data, list):
                        ticker_data = ticker_data[0]

                    # Parse expiry from symbol (YYMMDD format)
                    expiry_str = symbol.split("_")[1]
                    expiry_date = datetime.strptime("20" + expiry_str, "%Y%m%d")

                    mark_price = float(ticker_data["lastPrice"])
                    index_price = float(ticker_data["indexPrice"])

                    quarterly_contracts.append(
                        {
                            "symbol": symbol,
                            "expiry": expiry_date,
                            "futures_price": mark_price,
                            "spot_price": index_price,
                            "basis_absolute": mark_price - index_price,
                            "basis_percent": (mark_price - index_price)
                            / index_price
                            * 100,
                            "open_interest": float(ticker_data.get("openInterest", 0)),
                            "volume_24h": float(ticker_data.get("volume", 0)),
                        }
                    )

            # Sort by expiry
            quarterly_contracts.sort(key=lambda x: x["expiry"])
            return quarterly_contracts

        except Exception as e:
            self.log_error(f"Error fetching quarterly futures: {e}")
            return []


# Convenience functions
def fetch_binance_spot() -> Optional[float]:
    """Fetch BTC spot price from Binance."""
    return BinanceFetcher().fetch_spot_price()


def fetch_binance_futures() -> Optional[Dict[str, Any]]:
    """Fetch Binance perpetual futures."""
    return BinanceFetcher().fetch_perpetual_futures()
