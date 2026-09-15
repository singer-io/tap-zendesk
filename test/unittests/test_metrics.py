import unittest
from collections import defaultdict
from datetime import datetime, timedelta
from unittest.mock import patch

from tap_zendesk import metrics


class TestSecondsSinceDatetime(unittest.TestCase):

    def test_returns_elapsed_seconds(self):
        dt = datetime.utcnow() - timedelta(seconds=10)
        elapsed = metrics._seconds_since_datetime(dt)
        self.assertGreaterEqual(elapsed, 9)
        self.assertLessEqual(elapsed, 15)


class TestLogAggregateRates(unittest.TestCase):

    @patch('tap_zendesk.metrics.LOGGER')
    def test_logs_no_metrics_message_when_empty(self, mock_logger):
        metrics._log_aggregate_rates(300, defaultdict(list))
        mock_logger.info.assert_called_once_with("No zendesk metrics were captured")

    @patch('tap_zendesk.metrics.LOGGER')
    def test_logs_mean_min_max_total_for_each_metric(self, mock_logger):
        aggregate_rates = defaultdict(list)
        aggregate_rates["tickets"] = [1, 2, 3]
        metrics._log_aggregate_rates(300, aggregate_rates)
        # mean, min, max, total => 4 info calls for a single metric
        self.assertEqual(mock_logger.info.call_count, 4)


class TestAggregateRates(unittest.TestCase):

    @patch('tap_zendesk.metrics.LOGGER')
    def test_moves_window_counts_into_aggregate_rates(self, mock_logger):
        current_metrics_data = {
            'window_start_time': datetime.utcnow() - timedelta(seconds=500),
            'aggregate_rates': defaultdict(list),
            'window_counts': defaultdict(int, {"tickets": 5}),
        }

        metrics._aggregate_rates(300, current_metrics_data)

        self.assertEqual(current_metrics_data['aggregate_rates']["tickets"], [5])
        self.assertEqual(current_metrics_data['window_counts'], {})
        self.assertIsInstance(current_metrics_data['window_start_time'], datetime)


class TestMaybeAggregateRates(unittest.TestCase):

    @patch('tap_zendesk.metrics._aggregate_rates')
    def test_aggregates_when_capture_rate_elapsed(self, mock_aggregate_rates):
        current_metrics_data = {
            'window_start_time': datetime.utcnow() - timedelta(seconds=400),
        }
        metrics._maybe_aggregate_rates(300, current_metrics_data)
        mock_aggregate_rates.assert_called_once_with(300, current_metrics_data)

    @patch('tap_zendesk.metrics._aggregate_rates')
    def test_does_not_aggregate_before_capture_rate_elapsed(self, mock_aggregate_rates):
        current_metrics_data = {
            'window_start_time': datetime.utcnow(),
        }
        metrics._maybe_aggregate_rates(300, current_metrics_data)
        mock_aggregate_rates.assert_not_called()


class TestCaptureRaw(unittest.TestCase):

    @patch('tap_zendesk.metrics.LOGGER')
    def test_increments_window_count_for_metric(self, mock_logger):
        current_metrics_data = {'window_counts': defaultdict(int)}
        metrics._capture_raw(current_metrics_data, "tickets")
        metrics._capture_raw(current_metrics_data, "tickets")
        self.assertEqual(current_metrics_data['window_counts']["tickets"], 2)


class TestCapture(unittest.TestCase):

    def setUp(self):
        self._original_metrics_data = {
            'window_start_time': metrics.metrics_data['window_start_time'],
            'aggregate_rates': metrics.metrics_data['aggregate_rates'],
            'window_counts': metrics.metrics_data['window_counts'],
        }
        metrics.metrics_data['window_start_time'] = None
        metrics.metrics_data['aggregate_rates'] = defaultdict(list)
        metrics.metrics_data['window_counts'] = defaultdict(int)

    def tearDown(self):
        metrics.metrics_data['window_start_time'] = self._original_metrics_data['window_start_time']
        metrics.metrics_data['aggregate_rates'] = self._original_metrics_data['aggregate_rates']
        metrics.metrics_data['window_counts'] = self._original_metrics_data['window_counts']

    @patch('tap_zendesk.metrics.LOGGER')
    def test_first_capture_starts_window(self, mock_logger):
        metrics.capture("tickets")
        self.assertIsNotNone(metrics.metrics_data['window_start_time'])
        self.assertEqual(metrics.metrics_data['window_counts']["tickets"], 1)

    @patch('tap_zendesk.metrics.LOGGER')
    def test_subsequent_capture_does_not_reset_window(self, mock_logger):
        metrics.capture("tickets")
        first_window_start = metrics.metrics_data['window_start_time']
        metrics.capture("tickets")
        self.assertEqual(metrics.metrics_data['window_start_time'], first_window_start)
        self.assertEqual(metrics.metrics_data['window_counts']["tickets"], 2)


class TestLogAggregateRatesGlobal(unittest.TestCase):

    def setUp(self):
        self._original_metrics_data = {
            'window_start_time': metrics.metrics_data['window_start_time'],
            'aggregate_rates': metrics.metrics_data['aggregate_rates'],
            'window_counts': metrics.metrics_data['window_counts'],
        }

    def tearDown(self):
        metrics.metrics_data['window_start_time'] = self._original_metrics_data['window_start_time']
        metrics.metrics_data['aggregate_rates'] = self._original_metrics_data['aggregate_rates']
        metrics.metrics_data['window_counts'] = self._original_metrics_data['window_counts']

    @patch('tap_zendesk.metrics.LOGGER')
    def test_log_aggregate_rates_forces_aggregation(self, mock_logger):
        metrics.metrics_data['window_counts'] = defaultdict(int, {"tickets": 3})
        metrics.metrics_data['aggregate_rates'] = defaultdict(list)
        metrics.log_aggregate_rates()
        self.assertEqual(metrics.metrics_data['aggregate_rates']["tickets"], [3])
