"""
progress_service.py
-------------------
Thread-safe progress queue wrapping stdlib queue.Queue.
Posted by the background pipeline worker; consumed by Streamlit polling.
"""
import queue
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ProgressEvent:
    job_id: str
    chunk: int
    total_chunks_est: int
    rows: int
    rows_total: int
    status: str          # "running" | "done" | "error"
    error: str | None = None
    extra: dict = field(default_factory=dict)


class ProgressQueue:
    """Thin wrapper so callers don't import queue directly."""

    def __init__(self) -> None:
        self._q: queue.Queue = queue.Queue()

    def put(self, event: ProgressEvent) -> None:
        self._q.put(event)

    def get_all(self) -> list[ProgressEvent]:
        """Drain all available events without blocking."""
        events: list[ProgressEvent] = []
        while True:
            try:
                events.append(self._q.get_nowait())
            except queue.Empty:
                break
        return events

    def empty(self) -> bool:
        return self._q.empty()
