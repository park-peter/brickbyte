"""
Progress reporting for brickbyte sync operations.
"""

import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class ProgressEvent:
    """Progress event emitted during sync."""

    stream_name: str
    records_processed: int
    total_streams: int
    streams_completed: int
    elapsed_seconds: float


class ProgressReporter:
    """
    Reports sync progress via callback and optional tqdm bar.
    """

    def __init__(
        self,
        total_streams: int,
        callback: Optional[Callable[[ProgressEvent], None]] = None,
        use_tqdm: bool = False,
    ):
        self.total_streams = total_streams
        self.callback = callback
        self.streams_completed = 0
        self._start_time = time.monotonic()
        self._tqdm_bar = None
        self._records_by_stream: dict = {}
        self._lock = threading.Lock()

        if use_tqdm or self._is_notebook():
            try:
                from tqdm.auto import tqdm

                self._tqdm_bar = tqdm(
                    total=total_streams,
                    desc="brickbyte sync",
                    unit="stream",
                )
            except ImportError:
                pass

    def _is_notebook(self) -> bool:
        """Detect if running in a notebook environment."""
        try:
            from IPython import get_ipython

            shell = get_ipython()
            if shell is None:
                return False
            return "ZMQInteractiveShell" in type(shell).__name__
        except (ImportError, NameError):
            return False

    def record_processed(self, stream_name: str, count: int):
        """Called periodically during record processing."""
        with self._lock:
            self._records_by_stream[stream_name] = count
            streams_completed = self.streams_completed

        if count % 5000 == 0 and self.callback:
            event = ProgressEvent(
                stream_name=stream_name,
                records_processed=count,
                total_streams=self.total_streams,
                streams_completed=streams_completed,
                elapsed_seconds=time.monotonic() - self._start_time,
            )
            self.callback(event)

    def stream_completed(self, stream_name: str, records: int):
        """Called when a stream finishes."""
        with self._lock:
            self.streams_completed += 1
            self._records_by_stream[stream_name] = records
            streams_completed = self.streams_completed

        if self._tqdm_bar:
            self._tqdm_bar.update(1)
            self._tqdm_bar.set_postfix(stream=stream_name, records=records)

        if self.callback:
            event = ProgressEvent(
                stream_name=stream_name,
                records_processed=records,
                total_streams=self.total_streams,
                streams_completed=streams_completed,
                elapsed_seconds=time.monotonic() - self._start_time,
            )
            self.callback(event)

    def close(self):
        """Close tqdm bar if present."""
        if self._tqdm_bar:
            self._tqdm_bar.close()
