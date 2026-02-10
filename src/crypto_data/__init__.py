"""
Crypto Data Prep - Data preparation toolkit for cryptocurrency trading.

Provides data fetchers for multiple sources and backtesting capabilities.
"""

__version__ = "0.1.0"

from crypto_data.data.coinbase import CoinbaseFetcher, FearGreedFetcher
from crypto_data.data.binance import BinanceFetcher
from crypto_data.data.ibkr import IBKRFetcher, IBKRHistoricalFetcher

__all__ = [
    "CoinbaseFetcher",
    "FearGreedFetcher",
    "BinanceFetcher",
    "IBKRFetcher",
    "IBKRHistoricalFetcher",
]
