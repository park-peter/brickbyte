"""
Tests for progress reporting.
"""
from brickbyte._progress import ProgressReporter


class TestProgressReporter:
    def test_callback_invoked_on_stream_completion(self):
        events = []

        def callback(event):
            events.append(event)

        reporter = ProgressReporter(total_streams=3, callback=callback)
        reporter.stream_completed("users", 100)

        assert len(events) == 1
        assert events[0].stream_name == "users"
        assert events[0].records_processed == 100
        assert events[0].streams_completed == 1
        assert events[0].total_streams == 3

    def test_callback_invoked_every_5000_records(self):
        events = []

        def callback(event):
            events.append(event)

        reporter = ProgressReporter(total_streams=1, callback=callback)
        reporter.record_processed("users", 5000)

        assert len(events) == 1
        assert events[0].records_processed == 5000

    def test_callback_not_invoked_at_non_5000(self):
        events = []

        def callback(event):
            events.append(event)

        reporter = ProgressReporter(total_streams=1, callback=callback)
        reporter.record_processed("users", 1234)

        assert len(events) == 0

    def test_streams_completed_counter(self):
        reporter = ProgressReporter(total_streams=3)
        assert reporter.streams_completed == 0
        reporter.stream_completed("a", 10)
        assert reporter.streams_completed == 1
        reporter.stream_completed("b", 20)
        assert reporter.streams_completed == 2

    def test_close_without_tqdm(self):
        reporter = ProgressReporter(total_streams=1)
        reporter.close()  # Should not raise
