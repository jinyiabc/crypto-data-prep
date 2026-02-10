#!/usr/bin/env python3
"""
Configuration loader for BTC Basis Trade toolkit.

Consolidated from crypto_data_monitor.py and crypto_data_cli.py
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigLoader:
    """Load and manage configuration from JSON files."""

    DEFAULT_CONFIG = {
        "account_size": 200000,
        "spot_target_pct": 0.50,
        "futures_target_pct": 0.50,
        "funding_cost_annual": 0.05,
        "leverage": 1.0,
        "cme_contract_size": 5.0,
        "min_monthly_basis": 0.005,
        "alert_thresholds": {
            "stop_loss_basis": 0.002,
            "partial_exit_basis": 0.025,
            "full_exit_basis": 0.035,
            "strong_entry_basis": 0.01,
            "min_entry_basis": 0.005,
        },
        "ibkr": {
            "host": "127.0.0.1",
            "port": None,  # None = auto-detect (try 7497, 4002, 7496, 4001)
            "client_id": 1,
            "timeout": 10,
        },
    }

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration loader.

        Args:
            config_path: Path to config JSON file. Defaults to 'config/config.json'
        """
        self.config_path = config_path or self._find_config_file()
        self._config: Dict[str, Any] = {}
        self.load()

    def _find_config_file(self) -> str:
        """Find config file in standard locations."""
        search_paths = [
            Path("config/config.json"),
            Path("config.json"),
            Path.home() / ".crypto_data" / "config.json",
        ]

        for path in search_paths:
            if path.exists():
                return str(path)

        return "config/config.json"

    def load(self) -> Dict[str, Any]:
        """Load configuration from file."""
        path = Path(self.config_path)

        if path.exists():
            try:
                with open(path, "r") as f:
                    self._config = json.load(f)
                logging.debug(f"Config loaded from {self.config_path}")
            except json.JSONDecodeError as e:
                logging.warning(f"Invalid JSON in {self.config_path}: {e}")
                self._config = {}
            except Exception as e:
                logging.warning(f"Error loading config: {e}")
                self._config = {}
        else:
            logging.info(f"Config file not found: {self.config_path}, using defaults")
            self._config = {}

        return self._config

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value with fallback to defaults."""
        if key in self._config:
            return self._config[key]
        if key in self.DEFAULT_CONFIG:
            return self.DEFAULT_CONFIG[key]
        return default

    def get_all(self) -> Dict[str, Any]:
        """Get all configuration values merged with defaults."""
        merged = self.DEFAULT_CONFIG.copy()
        merged.update(self._config)
        return merged

    def save(self, config_data: Dict[str, Any] = None) -> bool:
        """Save configuration to file."""
        data = config_data or self._config
        path = Path(self.config_path)

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            logging.info(f"Config saved to {self.config_path}")
            return True
        except Exception as e:
            logging.error(f"Error saving config: {e}")
            return False

    @property
    def account_size(self) -> float:
        return self.get("account_size")

    @property
    def spot_target_pct(self) -> float:
        return self.get("spot_target_pct")

    @property
    def futures_target_pct(self) -> float:
        return self.get("futures_target_pct")

    @property
    def funding_cost_annual(self) -> float:
        return self.get("funding_cost_annual")

    @property
    def leverage(self) -> float:
        return self.get("leverage")

    @property
    def cme_contract_size(self) -> float:
        return self.get("cme_contract_size")

    @property
    def min_monthly_basis(self) -> float:
        return self.get("min_monthly_basis")

    @property
    def alert_thresholds(self) -> Dict[str, float]:
        return self.get("alert_thresholds")

    @property
    def ibkr(self) -> Dict[str, Any]:
        """Get IBKR connection settings."""
        return self.get("ibkr")
