#!/usr/bin/env python3
"""
Historical data utilities and rolling contract handling.

Consolidated from fetch_ibkr_historical_rolling.py
"""

import csv
from datetime import datetime
from typing import List, Dict, Any

from crypto_data.utils.expiry import (
    get_last_friday_of_month,
    generate_expiry_schedule,
    get_front_month_expiry,
)
from crypto_data.utils.logging import LoggingMixin


class RollingDataProcessor(LoggingMixin):
    """Process historical data with proper contract rolling logic."""

    def fix_rolling_expiry(self, input_file: str, output_file: str) -> bool:
        """
        Fix futures_expiry to use proper rolling contract logic.

        Reads historical CSV and reassigns futures_expiry to match
        the front-month contract that would have been trading on each date.

        Args:
            input_file: Input CSV path
            output_file: Output CSV path

        Returns:
            True if successful
        """
        self.log(f"Reading {input_file}...")

        try:
            # Read input CSV
            rows = []
            with open(input_file, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)

            self.log(f"[OK] Read {len(rows)} rows")

            # Determine date range
            start_date = datetime.strptime(rows[0]["date"], "%Y-%m-%d")
            end_date = datetime.strptime(rows[-1]["date"], "%Y-%m-%d")

            # Generate expiry schedule
            self.log("Generating CME futures expiry schedule...")
            expiry_schedule = generate_expiry_schedule(start_date, end_date)
            self.log(f"[OK] Generated {len(expiry_schedule)} expiry dates")

            # Fix each row
            self.log("Applying rolling contract logic...")
            fixed_rows = []
            current_expiry = None

            for row in rows:
                date = datetime.strptime(row["date"], "%Y-%m-%d")

                # Get front-month expiry for this date
                front_month_expiry = get_front_month_expiry(date, expiry_schedule)

                # Track when contract rolls
                if current_expiry is None:
                    current_expiry = front_month_expiry
                    self.log(
                        f"[*] Initial contract expiry: {current_expiry.strftime('%Y-%m-%d')}"
                    )

                if front_month_expiry != current_expiry:
                    days_to_old = (current_expiry - date).days
                    days_to_new = (front_month_expiry - date).days
                    self.log(f"[*] Contract ROLL on {date.strftime('%Y-%m-%d')}:")
                    self.log(
                        f"    Old expiry: {current_expiry.strftime('%Y-%m-%d')} ({days_to_old} days)"
                    )
                    self.log(
                        f"    New expiry: {front_month_expiry.strftime('%Y-%m-%d')} ({days_to_new} days)"
                    )
                    current_expiry = front_month_expiry

                # Update expiry
                fixed_rows.append(
                    {
                        "date": row["date"],
                        "spot_price": row["spot_price"],
                        "futures_price": row["futures_price"],
                        "futures_expiry": front_month_expiry.strftime("%Y-%m-%d"),
                    }
                )

            # Write output CSV
            self.log(f"Writing to {output_file}...")
            with open(output_file, "w", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["date", "spot_price", "futures_price", "futures_expiry"],
                )
                writer.writeheader()
                writer.writerows(fixed_rows)

            self.log(f"[OK] Written {len(fixed_rows)} rows")
            return True

        except FileNotFoundError:
            self.log_error(f"File not found: {input_file}")
            return False
        except Exception as e:
            self.log_error(f"Error processing file: {e}")
            return False

    def load_historical_csv(self, csv_path: str) -> List[Dict[str, Any]]:
        """
        Load historical basis data from CSV.

        Args:
            csv_path: Path to CSV file

        Returns:
            List of dicts with date, spot_price, futures_price, futures_expiry
        """
        data = []

        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(
                    {
                        "date": datetime.fromisoformat(row["date"]),
                        "spot_price": float(row["spot_price"]),
                        "futures_price": float(row["futures_price"]),
                        "futures_expiry": datetime.fromisoformat(row["futures_expiry"]),
                    }
                )

        return data

    def generate_sample_data(
        self,
        start_date: datetime,
        end_date: datetime,
        base_price: float = 50000,
        volatility: float = 0.02,
        avg_basis: float = 0.015,
    ) -> List[Dict[str, Any]]:
        """
        Generate synthetic historical data for testing.

        Args:
            start_date: Start date
            end_date: End date
            base_price: Starting BTC price
            volatility: Daily price volatility
            avg_basis: Average basis percentage

        Returns:
            List of synthetic historical data points
        """
        import random
        from datetime import timedelta

        data = []
        current_date = start_date
        price = base_price

        # Generate expiry schedule
        expiry_schedule = generate_expiry_schedule(start_date, end_date)

        while current_date <= end_date:
            # Simulate price movement (random walk)
            price_change = random.gauss(0, volatility) * price
            price = max(10000, price + price_change)

            # Simulate basis (typically positive, occasionally negative)
            basis_pct = max(-0.01, random.gauss(avg_basis, 0.01))
            futures_price = price * (1 + basis_pct)

            # Get front-month expiry
            expiry_date = get_front_month_expiry(current_date, expiry_schedule)

            data.append(
                {
                    "date": current_date,
                    "spot_price": price,
                    "futures_price": futures_price,
                    "futures_expiry": expiry_date,
                }
            )

            current_date += timedelta(days=1)

        return data

    def save_to_csv(self, data: List[Dict[str, Any]], output_file: str):
        """
        Save historical data to CSV.

        Args:
            data: List of data points
            output_file: Output file path
        """
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["date", "spot_price", "futures_price", "futures_expiry"]
            )
            writer.writeheader()

            for row in data:
                writer.writerow(
                    {
                        "date": row["date"].strftime("%Y-%m-%d")
                        if isinstance(row["date"], datetime)
                        else row["date"],
                        "spot_price": f"{row['spot_price']:.2f}",
                        "futures_price": f"{row['futures_price']:.2f}",
                        "futures_expiry": row["futures_expiry"].strftime("%Y-%m-%d")
                        if isinstance(row["futures_expiry"], datetime)
                        else row["futures_expiry"],
                    }
                )

        self.log(f"[OK] Saved {len(data)} rows to {output_file}")
