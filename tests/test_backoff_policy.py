"""Backoff policy: bounded exponential growth with jitter."""

from __future__ import annotations

import random

from app.provider import RetryPolicy

def test_backoff_grows_exponentially_and_is_capped() -> None:
    policy = RetryPolicy(max_attempts=8, base_seconds=0.1, max_seconds=1.0, jitter=0.0)
    rng = random.Random(0)

    delays = [policy.delay_for(attempt, rng) for attempt in range(1, 7)]

    assert delays == [0.1, 0.2, 0.4, 0.8, 1.0, 1.0], "exponential, capped at max_seconds"


def test_jitter_stays_within_the_backoff_window_and_varies() -> None:
    policy = RetryPolicy(max_attempts=8, base_seconds=1.0, max_seconds=10.0, jitter=1.0)
    rng = random.Random(7)

    samples = [policy.delay_for(3, rng) for _ in range(200)]

    backoff = 4.0  # 1.0 * 2**2
    assert all(0.0 <= sample <= backoff for sample in samples)
    assert len(set(samples)) > 100, "jitter must actually randomise the delay"


def test_zero_jitter_is_deterministic() -> None:
    policy = RetryPolicy(max_attempts=3, base_seconds=0.5, max_seconds=10.0, jitter=0.0)
    rng = random.Random(1)

    assert {policy.delay_for(2, rng) for _ in range(20)} == {1.0}
