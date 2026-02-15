#!/usr/bin/env python3
"""Tests for ConfigLoader pair configuration."""

import sys
import json
import pytest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_data.utils.config import ConfigLoader


class TestDefaultPairConfig:
    """Tests for default pair configuration."""

    def test_default_pairs_includes_btc(self):
        """Default config includes BTC pair."""
        loader = ConfigLoader("/nonexistent/config.json")
        pairs = loader.pairs
        assert "BTC" in pairs

    def test_default_btc_spot_config(self):
        """BTC default spot config is BTC.USD on PAXOS."""
        loader = ConfigLoader("/nonexistent/config.json")
        btc = loader.get_pair("BTC")
        assert btc["spot"]["symbol"] == "BTC"
        assert btc["spot"]["exchange"] == "PAXOS"
        assert btc["spot"]["currency"] == "USD"

    def test_default_btc_futures_config(self):
        """BTC default futures config is MBT on CME."""
        loader = ConfigLoader("/nonexistent/config.json")
        btc = loader.get_pair("BTC")
        assert btc["futures"]["symbol"] == "MBT"
        assert btc["futures"]["exchange"] == "CME"

    def test_default_pair_is_btc(self):
        """Default pair name is BTC."""
        loader = ConfigLoader("/nonexistent/config.json")
        assert loader.default_pair == "BTC"


class TestGetPair:
    """Tests for ConfigLoader.get_pair()."""

    def test_get_pair_by_name(self):
        """get_pair returns correct pair config."""
        loader = ConfigLoader("/nonexistent/config.json")
        pair = loader.get_pair("BTC")
        assert "spot" in pair
        assert "futures" in pair

    def test_get_pair_default_fallback(self):
        """get_pair with None uses default_pair."""
        loader = ConfigLoader("/nonexistent/config.json")
        pair = loader.get_pair()
        assert pair == loader.get_pair("BTC")

    def test_get_pair_unknown_raises_value_error(self):
        """get_pair raises ValueError for unknown pair."""
        loader = ConfigLoader("/nonexistent/config.json")
        with pytest.raises(ValueError, match="Unknown pair 'XRP'"):
            loader.get_pair("XRP")

    def test_available_pairs(self):
        """available_pairs returns list of pair names."""
        loader = ConfigLoader("/nonexistent/config.json")
        available = loader.available_pairs()
        assert "BTC" in available
        assert isinstance(available, list)


class TestPairMerge:
    """Tests for merging user-defined pairs with defaults."""

    def test_user_pairs_add_to_defaults(self, tmp_path):
        """User-defined pairs are added alongside defaults."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({
            "pairs": {
                "ETH": {
                    "spot": {"symbol": "ETH", "exchange": "PAXOS", "currency": "USD"},
                    "futures": {"symbol": "MET", "exchange": "CME"},
                }
            }
        }))

        loader = ConfigLoader(str(config_file))
        pairs = loader.pairs

        # Both default BTC and user-defined ETH should be present
        assert "BTC" in pairs
        assert "ETH" in pairs

    def test_user_can_override_default_pair(self, tmp_path):
        """User can override the default_pair setting."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({
            "pairs": {
                "ETH": {
                    "spot": {"symbol": "ETH", "exchange": "PAXOS", "currency": "USD"},
                    "futures": {"symbol": "MET", "exchange": "CME"},
                }
            },
            "default_pair": "ETH",
        }))

        loader = ConfigLoader(str(config_file))
        assert loader.default_pair == "ETH"
        pair = loader.get_pair()
        assert pair["spot"]["symbol"] == "ETH"

    def test_user_can_override_btc_pair(self, tmp_path):
        """User can override the default BTC pair config."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({
            "pairs": {
                "BTC": {
                    "spot": {"symbol": "BTC", "exchange": "PAXOS", "currency": "USD"},
                    "futures": {"symbol": "BTC", "exchange": "CME"},
                }
            }
        }))

        loader = ConfigLoader(str(config_file))
        btc = loader.get_pair("BTC")
        # User override: BTC (full-size) instead of MBT (micro)
        assert btc["futures"]["symbol"] == "BTC"

    def test_no_pairs_in_config_uses_defaults(self):
        """Config without pairs section uses defaults."""
        loader = ConfigLoader("/nonexistent/config.json")
        pairs = loader.pairs
        assert "BTC" in pairs
        assert pairs["BTC"]["spot"]["symbol"] == "BTC"


class TestDictMerge:
    """Tests for shallow dict merge behavior in get()."""

    def test_shallow_merge_for_ibkr(self, tmp_path):
        """User IBKR config merges with defaults."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({
            "ibkr": {"port": 7497}
        }))

        loader = ConfigLoader(str(config_file))
        ibkr = loader.ibkr
        # User override
        assert ibkr["port"] == 7497
        # Defaults preserved
        assert ibkr["host"] == "127.0.0.1"
        assert ibkr["client_id"] == 1

    def test_non_dict_values_not_merged(self, tmp_path):
        """Non-dict values are replaced, not merged."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({
            "account_size": 500000
        }))

        loader = ConfigLoader(str(config_file))
        assert loader.account_size == 500000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
