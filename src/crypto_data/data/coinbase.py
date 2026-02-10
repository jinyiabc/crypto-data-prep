#!/usr/bin/env python3
"""
Coinbase data fetcher for BTC spot prices.

Refactored from get_btc_prices.py and crypto_data_trade_analyzer.py
"""

import requests
from typing import Optional, Dict, Any

from crypto_data.data.base import BaseFetcher


class CoinbaseFetcher(BaseFetcher):
    """Fetch BTC spot prices from Coinbase API."""

    BASE_URL = "https://api.coinbase.com/v2"

    def __init__(self, timeout: int = 5):
        super().__init__(timeout)

    def fetch_spot_price(self, currency: str = "BTC", fiat: str = "USD") -> Optional[float]:
        """
        Fetch spot price from Coinbase.

        Args:
            currency: Cryptocurrency (default: BTC)
            fiat: Fiat currency (default: USD)

        Returns:
            Spot price or None if fetch failed
        """
        try:
            url = f"{self.BASE_URL}/prices/{currency}-{fiat}/spot"
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return float(data["data"]["amount"])
        except Exception as e:
            self.log_error(f"Error fetching Coinbase spot: {e}")
            return None

    def fetch_futures_price(self, expiry: str = None) -> Optional[Dict[str, Any]]:
        """
        Coinbase doesn't have futures - return None.

        Use IBKRFetcher or BinanceFetcher for futures.
        """
        return None

    def fetch_buy_price(self, currency: str = "BTC", fiat: str = "USD") -> Optional[float]:
        """Fetch buy price (includes spread)."""
        try:
            url = f"{self.BASE_URL}/prices/{currency}-{fiat}/buy"
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return float(data["data"]["amount"])
        except Exception as e:
            self.log_error(f"Error fetching Coinbase buy price: {e}")
            return None

    def fetch_sell_price(self, currency: str = "BTC", fiat: str = "USD") -> Optional[float]:
        """Fetch sell price (includes spread)."""
        try:
            url = f"{self.BASE_URL}/prices/{currency}-{fiat}/sell"
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return float(data["data"]["amount"])
        except Exception as e:
            self.log_error(f"Error fetching Coinbase sell price: {e}")
            return None


class FearGreedFetcher(BaseFetcher):
    """Fetch Fear & Greed Index from Alternative.me."""

    API_URL = "https://api.alternative.me/fng/"

    def __init__(self, timeout: int = 5):
        super().__init__(timeout)

    def fetch_spot_price(self) -> Optional[float]:
        """Not applicable - return None."""
        return None

    def fetch_futures_price(self, expiry: str = None) -> Optional[Dict[str, Any]]:
        """Not applicable - return None."""
        return None

    def fetch_index(self) -> Optional[float]:
        """
        Fetch Fear & Greed Index.

        Returns:
            Index value normalized to 0-1 range, or None if failed
        """
        try:
            response = requests.get(self.API_URL, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            # Returns 0-100, normalize to 0-1
            value = int(data["data"][0]["value"])
            return value / 100.0
        except Exception as e:
            self.log_error(f"Error fetching Fear & Greed Index: {e}")
            return None

    def fetch_index_with_classification(self) -> Optional[Dict[str, Any]]:
        """
        Fetch Fear & Greed Index with classification.

        Returns:
            Dictionary with value and classification
        """
        try:
            response = requests.get(self.API_URL, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()["data"][0]
            return {
                "value": int(data["value"]),
                "value_normalized": int(data["value"]) / 100.0,
                "classification": data["value_classification"],
                "timestamp": data["timestamp"],
            }
        except Exception as e:
            self.log_error(f"Error fetching Fear & Greed Index: {e}")
            return None


# Convenience functions for backwards compatibility
def fetch_coinbase_spot() -> Optional[float]:
    """Fetch BTC spot price from Coinbase."""
    return CoinbaseFetcher().fetch_spot_price()


def fetch_fear_greed_index() -> Optional[float]:
    """Fetch Fear & Greed Index."""
    return FearGreedFetcher().fetch_index()
