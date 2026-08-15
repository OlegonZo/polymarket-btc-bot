import json
import tempfile
import unittest
from pathlib import Path

from aggregate_analysis import AggregateSummary, load_summary


class AggregateAnalysisTests(unittest.TestCase):
    def test_published_summary_is_negative_edge(self) -> None:
        summary = AggregateSummary(
            total_rows=1496,
            resolved_rows=1485,
            pending_rows=11,
            unique_markets=181,
            win_rate=0.6943,
            average_entry_price=0.7038,
            total_virtual_pnl=-14.20,
        )
        summary.validate()

        self.assertAlmostEqual(summary.edge_per_share, -0.0095)
        self.assertAlmostEqual(summary.average_virtual_pnl, -14.20 / 1485)
        self.assertEqual(summary.conclusion, "not deployable")

    def test_inconsistent_counts_are_rejected(self) -> None:
        summary = AggregateSummary(
            total_rows=10,
            resolved_rows=8,
            pending_rows=1,
            unique_markets=3,
            win_rate=0.5,
            average_entry_price=0.6,
            total_virtual_pnl=-1.0,
        )
        with self.assertRaises(ValueError):
            summary.validate()

    def test_json_loader(self) -> None:
        payload = {
            "total_rows": 3,
            "resolved_rows": 2,
            "pending_rows": 1,
            "unique_markets": 1,
            "win_rate": 0.5,
            "average_entry_price": 0.6,
            "total_virtual_pnl": -0.2,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            self.assertAlmostEqual(load_summary(path).edge_per_share, -0.1)


if __name__ == "__main__":
    unittest.main()
