#!/usr/bin/env python3
"""
Futures expiry date utilities for CME Bitcoin futures.

Consolidated from fix_futures_expiry_rolling.py and fetch_ibkr_historical.py
"""

from datetime import datetime, timedelta
from typing import List


def get_last_friday_of_month(year: int, month: int) -> datetime:
    """
    Get last Friday of a given month.

    CME Bitcoin futures expire on the last Friday of the contract month.

    Args:
        year: Year (e.g., 2026)
        month: Month (1-12)

    Returns:
        datetime of last Friday of the month
    """
    # Last day of month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)

    last_day = next_month - timedelta(days=1)

    # Find last Friday (weekday 4 = Friday, Monday=0)
    days_back = (last_day.weekday() - 4) % 7
    last_friday = last_day - timedelta(days=days_back)

    return last_friday


def generate_expiry_schedule(
    start_date: datetime, end_date: datetime
) -> List[datetime]:
    """
    Generate all CME Bitcoin futures expiry dates in a date range.

    Args:
        start_date: Start of date range
        end_date: End of date range

    Returns:
        Sorted list of expiry dates (last Friday of each month)
    """
    expiries = []

    current = start_date.replace(day=1)  # Start of month
    end = end_date + timedelta(days=60)  # Include future expiries

    while current <= end:
        expiry = get_last_friday_of_month(current.year, current.month)
        expiries.append(expiry)

        # Next month
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)

    return sorted(list(set(expiries)))


def get_front_month_expiry(date: datetime, expiry_schedule: List[datetime]) -> datetime:
    """
    Get front-month expiry for a given date.

    Rule: Use the nearest expiry that is >= current date

    Args:
        date: Historical date
        expiry_schedule: List of all available expiry dates

    Returns:
        Front-month expiry date
    """
    for expiry in expiry_schedule:
        if expiry.date() >= date.date():
            return expiry

    # If no future expiry found, return the last one
    return expiry_schedule[-1]


def get_expiry_from_yyyymm(expiry_str: str) -> datetime:
    """
    Calculate approximate expiry date from YYYYMM format.

    Args:
        expiry_str: Expiry in YYYYMM format (e.g., '202603')

    Returns:
        Last Friday of the expiry month
    """
    year = int(expiry_str[:4])
    month = int(expiry_str[4:6])
    return get_last_friday_of_month(year, month)


def days_to_expiry(expiry_date: datetime, from_date: datetime = None) -> int:
    """
    Calculate days until futures expiry.

    Args:
        expiry_date: Futures expiry date
        from_date: Reference date (defaults to now)

    Returns:
        Number of days until expiry
    """
    reference = from_date or datetime.now()
    return (expiry_date - reference).days


def get_front_month_expiry_str(reference_date: datetime = None) -> str:
    """
    Get front-month futures expiry in YYYYMM format.

    CME Bitcoin futures expire on the last Friday of each month.
    If today is before the last Friday of the current month, use current month.
    Otherwise, roll to next month.

    Args:
        reference_date: Date to calculate from (default: now)

    Returns:
        Expiry string in YYYYMM format (e.g., '202603')
    """
    today = reference_date or datetime.now()

    # Get last Friday of current month
    current_month_expiry = get_last_friday_of_month(today.year, today.month)

    # If today is before the expiry, use current month
    # Otherwise roll to next month
    if today.date() < current_month_expiry.date():
        return f"{today.year:04d}{today.month:02d}"
    else:
        # Next month
        if today.month == 12:
            return f"{today.year + 1:04d}01"
        else:
            return f"{today.year:04d}{today.month + 1:02d}"
