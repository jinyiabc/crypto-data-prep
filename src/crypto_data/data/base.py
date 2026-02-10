#!/usr/bin/env python3
"""
Base fetcher class for data sources.

Provides common interface and utilities for all data fetchers.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict, Any

from crypto_data.utils.logging import LoggingMixin


class BaseFetcher(ABC, LoggingMixin):
    """Abstract base class for data fetchers."""

    def __init__(self, timeout: int = 10):
        """
        Initialize fetcher.

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout

    @abstractmethod
    def fetch_spot_price(self) -> Optional[float]:
        """
        Fetch current spot price.

        Returns:
            Spot price or None if fetch failed
        """
        pass

    @abstractmethod
    def fetch_futures_price(self, expiry: str = None) -> Optional[Dict[str, Any]]:
        """
        Fetch futures price.

        Args:
            expiry: Contract expiry (format depends on exchange)

        Returns:
            Dictionary with futures data or None if fetch failed
        """
        pass

    def fetch_basis_data(self, expiry: str = None) -> Optional[Dict[str, Any]]:
        """
        Fetch complete basis data (spot + futures + calculations).

        Args:
            expiry: Contract expiry

        Returns:
            Dictionary with complete basis data or None
        """
        spot = self.fetch_spot_price()
        futures = self.fetch_futures_price(expiry)

        if not spot or not futures:
            return None

        futures_price = futures.get("futures_price", futures.get("mark_price", 0))

        basis_absolute = futures_price - spot
        basis_percent = basis_absolute / spot if spot > 0 else 0

        return {
            "spot_price": spot,
            "futures_price": futures_price,
            "basis_absolute": basis_absolute,
            "basis_percent": basis_percent,
            "basis_percent_display": basis_percent * 100,
            "timestamp": datetime.now(),
            **futures,
        }
