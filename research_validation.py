"""Descriptive research checks; no strategy changes or launch authority.

Whole-day resampling preserves within-day dependence, but assumes independent
days. This is sensitivity analysis until an inference plan is approved.
"""
import math
import random
from statistics import mean
from typing import Mapping, Sequence


def day_cluster_sensitivity(groups: Mapping[str, Sequence[float]], *, samples: int = 4000,
                            seed: int = 20260910) -> dict:
    blocks = [list(groups[key]) for key in sorted(groups) if groups[key]]
    if samples <= 0 or any(not math.isfinite(value) for block in blocks for value in block):
        raise ValueError("finite values and positive sample count required")
    result = {"status": "DESCRIPTIVE_NOT_AN_EDGE_GATE", "day_blocks": len(blocks),
              "episode_representatives": sum(map(len, blocks)), "interval": None,
              "assumption": "day blocks independent; within-day dependence retained",
              "samples": samples, "seed": seed}
    if len(blocks) < 2:
        result["status"] = "INSUFFICIENT_DAY_BLOCKS"
        return result
    rng = random.Random(seed)
    totals = [(sum(block), len(block)) for block in blocks]
    estimates = []
    for _ in range(samples):
        selected = [totals[rng.randrange(len(totals))] for _ in totals]
        estimates.append(sum(value for value, _ in selected) / sum(n for _, n in selected))
    estimates.sort()
    result["interval"] = {"estimate": mean([value for block in blocks for value in block]),
                          "lower": estimates[int(.025 * (samples - 1))],
                          "upper": estimates[int(.975 * (samples - 1))]}
    return result
