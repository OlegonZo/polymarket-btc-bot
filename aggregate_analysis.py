"""Recompute published aggregate Polymarket research metrics.

This module operates on sanitized aggregate inputs only. It is not the private
resolver, raw dataset, execution code, or an independent backtest.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AggregateSummary:
    total_rows: int
    resolved_rows: int
    pending_rows: int
    unique_markets: int
    win_rate: float
    average_entry_price: float
    total_virtual_pnl: float

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "AggregateSummary":
        summary = cls(
            total_rows=int(value["total_rows"]),
            resolved_rows=int(value["resolved_rows"]),
            pending_rows=int(value["pending_rows"]),
            unique_markets=int(value["unique_markets"]),
            win_rate=float(value["win_rate"]),
            average_entry_price=float(value["average_entry_price"]),
            total_virtual_pnl=float(value["total_virtual_pnl"]),
        )
        summary.validate()
        return summary

    def validate(self) -> None:
        if self.total_rows <= 0 or self.resolved_rows <= 0:
            raise ValueError("row counts must be positive")
        if self.resolved_rows + self.pending_rows != self.total_rows:
            raise ValueError("resolved_rows + pending_rows must equal total_rows")
        if not 0 < self.unique_markets <= self.resolved_rows:
            raise ValueError("unique_markets must be within the resolved sample")
        if not 0.0 <= self.win_rate <= 1.0:
            raise ValueError("win_rate must be between 0 and 1")
        if not 0.0 <= self.average_entry_price <= 1.0:
            raise ValueError("average_entry_price must be between 0 and 1")

    @property
    def edge_per_share(self) -> float:
        return self.win_rate - self.average_entry_price

    @property
    def average_virtual_pnl(self) -> float:
        return self.total_virtual_pnl / self.resolved_rows

    @property
    def conclusion(self) -> str:
        return "not deployable" if self.edge_per_share <= 0 else "requires further validation"


def load_summary(path: Path) -> AggregateSummary:
    with path.open("r", encoding="utf-8") as handle:
        return AggregateSummary.from_mapping(json.load(handle))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("summary", type=Path)
    args = parser.parse_args()

    summary = load_summary(args.summary)
    print(f"Resolved rows: {summary.resolved_rows}")
    print(f"Unique markets: {summary.unique_markets}")
    print(f"Edge per share: {summary.edge_per_share:.4%}")
    print(f"Average virtual PnL: {summary.average_virtual_pnl:.5f}")
    print(f"Conclusion: {summary.conclusion}")


if __name__ == "__main__":
    main()
