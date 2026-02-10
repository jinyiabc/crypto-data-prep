#!/usr/bin/env python3
"""
Backtesting engine for basis trade strategy.

Standalone version without external dependencies.
"""

import csv
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import statistics
from enum import Enum


class Signal(Enum):
    """Trading signals."""
    STRONG_ENTRY = "strong_entry"
    ACCEPTABLE_ENTRY = "acceptable_entry"
    PARTIAL_EXIT = "partial_exit"
    FULL_EXIT = "full_exit"
    STOP_LOSS = "stop_loss"
    NO_ENTRY = "no_entry"


@dataclass
class Trade:
    """Represents a single basis trade."""

    entry_date: datetime
    entry_spot: float
    entry_futures: float
    entry_basis: float
    exit_date: Optional[datetime] = None
    exit_spot: Optional[float] = None
    exit_futures: Optional[float] = None
    exit_basis: Optional[float] = None
    position_size: float = 1.0
    funding_cost: float = 0.0
    realized_pnl: Optional[float] = None
    status: str = "open"  # open, closed, stopped_out, forced_close

    @property
    def holding_days(self) -> int:
        """Days trade was held."""
        if self.exit_date:
            return (self.exit_date - self.entry_date).days
        return 0

    @property
    def return_pct(self) -> Optional[float]:
        """Return percentage."""
        if self.realized_pnl:
            return self.realized_pnl / (self.entry_spot * self.position_size)
        return None

    @property
    def annualized_return(self) -> Optional[float]:
        """Annualized return."""
        if self.return_pct and self.holding_days > 0:
            return self.return_pct * (365 / self.holding_days)
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "entry_date": self.entry_date.isoformat(),
            "exit_date": self.exit_date.isoformat() if self.exit_date else None,
            "entry_basis": self.entry_basis,
            "exit_basis": self.exit_basis,
            "holding_days": self.holding_days,
            "return_pct": self.return_pct * 100 if self.return_pct else None,
            "annualized_return": self.annualized_return * 100
            if self.annualized_return
            else None,
            "status": self.status,
        }


@dataclass
class BacktestResult:
    """Results from backtesting."""

    trades: List[Trade] = field(default_factory=list)
    total_return: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    initial_capital: float = 200000

    @property
    def win_rate(self) -> float:
        """Win rate percentage."""
        if self.total_trades == 0:
            return 0.0
        return self.winning_trades / self.total_trades

    @property
    def profit_factor(self) -> float:
        """Ratio of gross profit to gross loss."""
        if abs(self.avg_loss) < 0.0001:
            return float("inf")
        return abs(self.avg_win / self.avg_loss)

    @property
    def final_capital(self) -> float:
        """Final capital after all trades."""
        return self.initial_capital * (1 + self.total_return)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "summary": {
                "initial_capital": self.initial_capital,
                "final_capital": self.final_capital,
                "total_return": self.total_return * 100,
                "total_trades": self.total_trades,
                "winning_trades": self.winning_trades,
                "losing_trades": self.losing_trades,
                "win_rate": self.win_rate * 100,
                "avg_win": self.avg_win * 100,
                "avg_loss": self.avg_loss * 100,
                "profit_factor": self.profit_factor,
                "max_drawdown": self.max_drawdown * 100,
                "sharpe_ratio": self.sharpe_ratio,
                "start_date": self.start_date.isoformat() if self.start_date else None,
                "end_date": self.end_date.isoformat() if self.end_date else None,
            },
            "trades": [t.to_dict() for t in self.trades],
        }


class Backtester:
    """Backtesting engine for basis trade strategy."""

    def __init__(self, config=None):
        """
        Initialize backtester.

        Args:
            config: Configuration object with account_size, funding_cost_annual, etc.
        """
        self.config = config
        self.account_size = getattr(config, "account_size", 200000)
        self.funding_cost_annual = getattr(config, "funding_cost_annual", 0.05)
        self.min_monthly_basis = getattr(config, "min_monthly_basis", 0.005)

    def generate_signal(
        self, spot_price: float, futures_price: float, days_to_expiry: int
    ) -> Signal:
        """
        Generate trading signal based on basis.

        Args:
            spot_price: Current spot price
            futures_price: Current futures price
            days_to_expiry: Days until futures expiry

        Returns:
            Trading signal
        """
        if days_to_expiry <= 0:
            days_to_expiry = 1

        basis_pct = (futures_price - spot_price) / spot_price
        monthly_basis = basis_pct * (30 / days_to_expiry)

        # Stop loss conditions
        if basis_pct < 0:
            return Signal.STOP_LOSS
        if monthly_basis < 0.002:
            return Signal.STOP_LOSS

        # Exit conditions
        if monthly_basis > 0.035:
            return Signal.FULL_EXIT
        if monthly_basis > 0.025:
            return Signal.PARTIAL_EXIT

        # Entry conditions
        if monthly_basis > 0.01:
            return Signal.STRONG_ENTRY
        if monthly_basis > 0.005:
            return Signal.ACCEPTABLE_ENTRY

        return Signal.NO_ENTRY

    def load_historical_data(self, csv_path: str) -> List[Dict]:
        """
        Load historical basis data from CSV.

        Expected columns: date, spot_price, futures_price, futures_expiry

        Args:
            csv_path: Path to CSV file

        Returns:
            List of data points
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
    ) -> List[Dict]:
        """
        Generate synthetic data for testing.

        Args:
            start_date: Start date
            end_date: End date
            base_price: Starting price

        Returns:
            List of synthetic data points
        """
        import random

        data = []
        current_date = start_date
        price = base_price

        while current_date <= end_date:
            # Simulate price movement
            price_change = random.gauss(0, 0.02) * price
            price = max(10000, price + price_change)

            # Simulate basis (typically positive contango)
            basis_pct = max(-0.01, random.gauss(0.015, 0.01))
            futures_price = price * (1 + basis_pct)

            # Futures expiry (30 days out)
            expiry_date = current_date + timedelta(days=30)

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

    def run_backtest(
        self,
        historical_data: List[Dict],
        holding_days: int = 30,
    ) -> BacktestResult:
        """
        Run backtest on historical data.

        Args:
            historical_data: List of historical data points
            holding_days: Maximum holding period

        Returns:
            BacktestResult with all metrics
        """
        result = BacktestResult(initial_capital=self.account_size)
        current_trade: Optional[Trade] = None
        equity_curve = [result.initial_capital]
        daily_returns = []

        result.start_date = historical_data[0]["date"]
        result.end_date = historical_data[-1]["date"]

        for data_point in historical_data:
            spot_price = data_point["spot_price"]
            futures_price = data_point["futures_price"]
            expiry = data_point["futures_expiry"]
            current_date = data_point["date"]

            days_to_expiry = (expiry - current_date).days
            signal = self.generate_signal(spot_price, futures_price, days_to_expiry)

            basis_absolute = futures_price - spot_price

            # Check if we have an open trade
            if current_trade:
                trade_holding_days = (current_date - current_trade.entry_date).days

                # Exit conditions
                should_exit = False

                if signal in [Signal.STOP_LOSS, Signal.FULL_EXIT]:
                    should_exit = True
                    current_trade.status = (
                        "stopped_out" if signal == Signal.STOP_LOSS else "closed"
                    )
                elif trade_holding_days >= holding_days:
                    should_exit = True
                    current_trade.status = "closed"

                if should_exit:
                    # Close trade
                    current_trade.exit_date = current_date
                    current_trade.exit_spot = spot_price
                    current_trade.exit_futures = futures_price
                    current_trade.exit_basis = basis_absolute

                    # Calculate P&L (long spot, short futures)
                    spot_pnl = (
                        current_trade.exit_spot - current_trade.entry_spot
                    ) * current_trade.position_size
                    futures_pnl = (
                        current_trade.entry_futures - current_trade.exit_futures
                    ) * current_trade.position_size

                    # Funding cost
                    holding_days_actual = (
                        current_trade.exit_date - current_trade.entry_date
                    ).days
                    funding_cost = (
                        (self.funding_cost_annual / 365)
                        * holding_days_actual
                        * (current_trade.entry_spot * current_trade.position_size)
                    )

                    current_trade.realized_pnl = spot_pnl + futures_pnl - funding_cost

                    # Update equity
                    equity_curve.append(equity_curve[-1] + current_trade.realized_pnl)

                    # Track daily return
                    if len(equity_curve) > 1:
                        daily_return = (
                            equity_curve[-1] - equity_curve[-2]
                        ) / equity_curve[-2]
                        daily_returns.append(daily_return)

                    result.trades.append(current_trade)
                    current_trade = None

            # Entry conditions (no open trade)
            if current_trade is None:
                if signal in [Signal.STRONG_ENTRY, Signal.ACCEPTABLE_ENTRY]:
                    current_trade = Trade(
                        entry_date=current_date,
                        entry_spot=spot_price,
                        entry_futures=futures_price,
                        entry_basis=basis_absolute,
                        position_size=1.0,
                    )

        # Close any remaining open trade
        if current_trade:
            last_data = historical_data[-1]
            current_trade.exit_date = last_data["date"]
            current_trade.exit_spot = last_data["spot_price"]
            current_trade.exit_futures = last_data["futures_price"]
            current_trade.status = "forced_close"

            spot_pnl = (
                current_trade.exit_spot - current_trade.entry_spot
            ) * current_trade.position_size
            futures_pnl = (
                current_trade.entry_futures - current_trade.exit_futures
            ) * current_trade.position_size
            holding_days_actual = (
                current_trade.exit_date - current_trade.entry_date
            ).days
            funding_cost = (
                (self.funding_cost_annual / 365)
                * holding_days_actual
                * (current_trade.entry_spot * current_trade.position_size)
            )
            current_trade.realized_pnl = spot_pnl + futures_pnl - funding_cost

            result.trades.append(current_trade)

        # Calculate statistics
        result.total_trades = len(result.trades)

        if result.total_trades > 0:
            wins = [t for t in result.trades if t.realized_pnl and t.realized_pnl > 0]
            losses = [t for t in result.trades if t.realized_pnl and t.realized_pnl < 0]

            result.winning_trades = len(wins)
            result.losing_trades = len(losses)

            if wins:
                result.avg_win = sum(t.return_pct for t in wins if t.return_pct) / len(
                    wins
                )
            if losses:
                result.avg_loss = sum(
                    t.return_pct for t in losses if t.return_pct
                ) / len(losses)

            # Total return
            result.total_return = (equity_curve[-1] - equity_curve[0]) / equity_curve[0]

            # Max drawdown
            peak = equity_curve[0]
            max_dd = 0
            for equity in equity_curve:
                if equity > peak:
                    peak = equity
                dd = (peak - equity) / peak
                if dd > max_dd:
                    max_dd = dd
            result.max_drawdown = max_dd

            # Sharpe ratio
            if daily_returns and len(daily_returns) > 1:
                avg_return = statistics.mean(daily_returns)
                std_return = statistics.stdev(daily_returns)
                if std_return > 0:
                    result.sharpe_ratio = (avg_return / std_return) * (365**0.5)

        return result
