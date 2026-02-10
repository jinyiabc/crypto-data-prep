#!/usr/bin/env python3
"""
I/O utilities for report writing and data export.

Consolidated from crypto_data_trade_analyzer.py report generation.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional


class ReportWriter:
    """Write analysis reports in various formats."""

    def __init__(self, output_dir: str = "output"):
        """
        Initialize report writer.

        Args:
            output_dir: Base output directory
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _get_timestamp(self) -> str:
        """Get formatted timestamp for filenames."""
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def _get_filepath(self, prefix: str, extension: str, subdir: str = None) -> Path:
        """
        Get full filepath for output file.

        Args:
            prefix: Filename prefix
            extension: File extension (without dot)
            subdir: Optional subdirectory

        Returns:
            Full Path object
        """
        timestamp = self._get_timestamp()
        filename = f"{prefix}_{timestamp}.{extension}"

        if subdir:
            output_path = self.output_dir / subdir
            output_path.mkdir(parents=True, exist_ok=True)
        else:
            output_path = self.output_dir

        return output_path / filename

    def write_text_report(
        self,
        content: str,
        prefix: str = "report",
        subdir: str = "analysis",
    ) -> str:
        """
        Write text report to file.

        Args:
            content: Report content
            prefix: Filename prefix
            subdir: Subdirectory within output dir

        Returns:
            Path to created file
        """
        filepath = self._get_filepath(prefix, "txt", subdir)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

        return str(filepath)

    def write_json_report(
        self,
        data: Dict[str, Any],
        prefix: str = "data",
        subdir: str = "analysis",
    ) -> str:
        """
        Write JSON data to file.

        Args:
            data: Dictionary to serialize
            prefix: Filename prefix
            subdir: Subdirectory within output dir

        Returns:
            Path to created file
        """
        filepath = self._get_filepath(prefix, "json", subdir)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

        return str(filepath)

    def write_analysis_output(
        self,
        report: str,
        data: Dict[str, Any],
        prefix: str = "crypto_data_analysis",
    ) -> Dict[str, str]:
        """
        Write both text and JSON output for analysis.

        Args:
            report: Text report content
            data: Structured data dictionary
            prefix: Filename prefix

        Returns:
            Dictionary with paths to both files
        """
        txt_path = self.write_text_report(report, prefix, "analysis")
        json_path = self.write_json_report(data, prefix, "analysis")

        return {
            "text_report": txt_path,
            "json_data": json_path,
        }

    def write_backtest_result(
        self,
        data: Dict[str, Any],
        prefix: str = "backtest_result",
    ) -> str:
        """
        Write backtest results to JSON file.

        Args:
            data: Backtest result dictionary
            prefix: Filename prefix

        Returns:
            Path to created file
        """
        return self.write_json_report(data, prefix, "backtests")

    def write_log(
        self,
        content: str,
        prefix: str = "log",
    ) -> str:
        """
        Write log content to file.

        Args:
            content: Log content
            prefix: Filename prefix

        Returns:
            Path to created file
        """
        return self.write_text_report(content, prefix, "logs")
