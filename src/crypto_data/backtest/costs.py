#!/usr/bin/env python3
"""
Enhanced cost calculation for basis trade backtesting.

Refactored from backtest_costs_enhanced.py
"""

from dataclasses import dataclass
from typing import Dict


@dataclass
class TradingCosts:
    """All costs involved in a basis trade."""

    # Transaction costs (one-time)
    spot_entry_commission: float = 0.0
    spot_exit_commission: float = 0.0
    futures_entry_commission: float = 0.0
    futures_exit_commission: float = 0.0
    etf_entry_commission: float = 0.0
    etf_exit_commission: float = 0.0

    # Slippage costs (one-time)
    spot_entry_slippage: float = 0.0
    spot_exit_slippage: float = 0.0
    futures_entry_slippage: float = 0.0
    futures_exit_slippage: float = 0.0

    # Holding costs (daily/ongoing)
    funding_cost: float = 0.0
    etf_expense_ratio: float = 0.0  # Daily ETF management fee

    @property
    def total_entry_costs(self) -> float:
        """Total costs to enter position."""
        return (
            self.spot_entry_commission
            + self.futures_entry_commission
            + self.etf_entry_commission
            + self.spot_entry_slippage
            + self.futures_entry_slippage
        )

    @property
    def total_exit_costs(self) -> float:
        """Total costs to exit position."""
        return (
            self.spot_exit_commission
            + self.futures_exit_commission
            + self.etf_exit_commission
            + self.spot_exit_slippage
            + self.futures_exit_slippage
        )

    @property
    def total_holding_costs(self) -> float:
        """Total holding costs over trade duration."""
        return self.funding_cost + self.etf_expense_ratio

    @property
    def total_costs(self) -> float:
        """All costs combined."""
        return self.total_entry_costs + self.total_exit_costs + self.total_holding_costs


def calculate_comprehensive_costs(
    entry_spot: float,
    exit_spot: float,
    entry_futures: float,
    exit_futures: float,
    position_size: float,
    holding_days: int,
    use_etf: bool = True,
    funding_rate_annual: float = 0.05,
    etf_expense_ratio_annual: float = 0.0025,
) -> Dict[str, float]:
    """
    Calculate all costs for a basis trade.

    Args:
        entry_spot: Spot price at entry
        exit_spot: Spot price at exit
        entry_futures: Futures price at entry
        exit_futures: Futures price at exit
        position_size: Position size in BTC
        holding_days: Days trade was held
        use_etf: True if using ETF (IBIT/FBTC), False if direct spot BTC
        funding_rate_annual: Annual funding cost (default 5%)
        etf_expense_ratio_annual: ETF expense ratio (default 0.25%)

    Returns:
        Dictionary with detailed cost breakdown
    """
    costs = TradingCosts()

    # ==================================================================
    # 1. COMMISSION COSTS
    # ==================================================================

    if use_etf:
        # ETF Trading (e.g., IBIT via IBKR)
        # Typical: $0.005 per share, min $1, max 1% of trade value
        etf_commission_rate = 0.0005  # 0.05% typical

        costs.etf_entry_commission = max(
            1, entry_spot * position_size * etf_commission_rate
        )
        costs.etf_exit_commission = max(
            1, exit_spot * position_size * etf_commission_rate
        )
    else:
        # Direct Spot BTC (e.g., Coinbase, Kraken)
        # Maker: 0.40%, Taker: 0.60% (Coinbase Pro tier)
        spot_commission_rate = 0.004  # 0.4% maker fee

        costs.spot_entry_commission = entry_spot * position_size * spot_commission_rate
        costs.spot_exit_commission = exit_spot * position_size * spot_commission_rate

    # CME Bitcoin Futures Commission
    # Typical: $1.50-$2.50 per contract per side
    # 1 contract = 5 BTC, so for 1 BTC we have 0.2 contracts
    contracts = position_size / 5.0
    futures_commission_per_contract = 2.00  # $2 per contract

    costs.futures_entry_commission = contracts * futures_commission_per_contract
    costs.futures_exit_commission = contracts * futures_commission_per_contract

    # ==================================================================
    # 2. SLIPPAGE COSTS
    # ==================================================================

    if use_etf:
        # ETF slippage (very low, tight spreads)
        etf_slippage_rate = 0.0001  # 0.01% (1 basis point)
        costs.spot_entry_slippage = entry_spot * position_size * etf_slippage_rate
        costs.spot_exit_slippage = exit_spot * position_size * etf_slippage_rate
    else:
        # Spot BTC slippage
        spot_slippage_rate = 0.0005  # 0.05%
        costs.spot_entry_slippage = entry_spot * position_size * spot_slippage_rate
        costs.spot_exit_slippage = exit_spot * position_size * spot_slippage_rate

    # Futures slippage (CME is very liquid)
    futures_slippage_rate = 0.0002  # 0.02%
    costs.futures_entry_slippage = entry_futures * position_size * futures_slippage_rate
    costs.futures_exit_slippage = exit_futures * position_size * futures_slippage_rate

    # ==================================================================
    # 3. FUNDING COST (HOLDING COST)
    # ==================================================================

    position_value = entry_spot * position_size
    costs.funding_cost = (funding_rate_annual / 365) * holding_days * position_value

    # ==================================================================
    # 4. ETF EXPENSE RATIO (if using ETF)
    # ==================================================================

    if use_etf:
        costs.etf_expense_ratio = (
            (etf_expense_ratio_annual / 365) * holding_days * position_value
        )

    # ==================================================================
    # SUMMARY
    # ==================================================================

    return {
        # Entry costs
        "spot_entry_commission": costs.spot_entry_commission,
        "etf_entry_commission": costs.etf_entry_commission,
        "futures_entry_commission": costs.futures_entry_commission,
        "spot_entry_slippage": costs.spot_entry_slippage,
        "futures_entry_slippage": costs.futures_entry_slippage,
        "total_entry_costs": costs.total_entry_costs,
        # Exit costs
        "spot_exit_commission": costs.spot_exit_commission,
        "etf_exit_commission": costs.etf_exit_commission,
        "futures_exit_commission": costs.futures_exit_commission,
        "spot_exit_slippage": costs.spot_exit_slippage,
        "futures_exit_slippage": costs.futures_exit_slippage,
        "total_exit_costs": costs.total_exit_costs,
        # Holding costs
        "funding_cost": costs.funding_cost,
        "etf_expense_ratio": costs.etf_expense_ratio,
        "total_holding_costs": costs.total_holding_costs,
        # Grand total
        "total_all_costs": costs.total_costs,
    }


def calculate_net_pnl(
    entry_spot: float,
    exit_spot: float,
    entry_futures: float,
    exit_futures: float,
    position_size: float,
    holding_days: int,
    use_etf: bool = True,
) -> Dict[str, float]:
    """
    Calculate net P&L including all costs.

    Args:
        entry_spot: Spot price at entry
        exit_spot: Spot price at exit
        entry_futures: Futures price at entry
        exit_futures: Futures price at exit
        position_size: Position size in BTC
        holding_days: Days trade was held
        use_etf: True if using ETF

    Returns:
        Dictionary with P&L breakdown
    """
    # Gross P&L
    spot_pnl = (exit_spot - entry_spot) * position_size
    futures_pnl = (entry_futures - exit_futures) * position_size
    gross_pnl = spot_pnl + futures_pnl

    # Get costs
    costs = calculate_comprehensive_costs(
        entry_spot,
        exit_spot,
        entry_futures,
        exit_futures,
        position_size,
        holding_days,
        use_etf,
    )

    # Net P&L
    net_pnl = gross_pnl - costs["total_all_costs"]
    net_return_pct = (net_pnl / (entry_spot * position_size)) * 100
    annualized_return = net_return_pct * (365 / holding_days) if holding_days > 0 else 0

    return {
        "spot_pnl": spot_pnl,
        "futures_pnl": futures_pnl,
        "gross_pnl": gross_pnl,
        "total_costs": costs["total_all_costs"],
        "net_pnl": net_pnl,
        "net_return_pct": net_return_pct,
        "annualized_return": annualized_return,
        "cost_breakdown": costs,
    }
