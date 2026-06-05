"""Topic definitions for the Animal Crossing themed workshop.

- ``home-upgrade-quotes`` is created here but only written to by the
  ``publish_event`` tool when we accept a quote.
- ``abd-balance`` is steady (1 msg/s of player balance updates at the
  Automatic Bell Dispenser, with occasional suspicious lump-sum deposits);
  used by TANUKI's ABD balance multiplier.
- ``catch-log`` is slow (one fish/bug catch by the player every ~5 minutes);
  used by TANUKI's catch value multiplier.
- ``island-visitors`` is slow (one special visitor per day, determined by
  date); Flick and CJ trigger a 1.5x catch sell-price multiplier.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    messages_per_second: float


TOPICS: list[TopicSpec] = [
    TopicSpec("home-upgrade-quotes", partitions=1, messages_per_second=0.0),
    TopicSpec("abd-balance", partitions=1, messages_per_second=1.0),
    TopicSpec("catch-log", partitions=1, messages_per_second=1.0 / 300.0),  # 1 msg / 5 min
    TopicSpec("island-visitors", partitions=1, messages_per_second=0.1),
]
