"""Exponential backoff with full jitter (FR-01; Brooker 2015, "Exponential Backoff and Jitter")."""

from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass
class Backoff:
    """Delay before reconnect attempt n is uniform on [0, min(cap, base * factor**n)].

    ``reset()`` is called once a connection has been stable for ``stable_reset_s``.
    """

    base_s: float = 1.0
    factor: float = 2.0
    cap_s: float = 60.0
    jitter: str = "full"
    rng: random.Random = field(default_factory=random.Random)
    attempt: int = 0

    def ceiling(self, attempt: int) -> float:
        """Upper bound of the delay for ``attempt`` (0-based)."""
        return min(self.cap_s, self.base_s * self.factor ** min(attempt, 64))

    def next_delay(self) -> float:
        top = self.ceiling(self.attempt)
        self.attempt += 1
        return top if self.jitter == "none" else self.rng.uniform(0.0, top)

    def reset(self) -> None:
        self.attempt = 0
