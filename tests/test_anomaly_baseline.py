import statistics

import pytest

from ducat_lakehouse.rules import outflow_z_score

RECENT = [40.0, 42.0, 38.0, 41.0, 39.0]
HISTORY = [10.0, 12.0, 11.0, 9.0, 13.0, *RECENT]


def test_recent_window_is_used_when_it_has_enough_history():
    expected = (200 - statistics.fmean(RECENT)) / statistics.stdev(RECENT)
    assert outflow_z_score(200, RECENT, HISTORY) == pytest.approx(expected)


def test_sparse_recent_window_falls_back_to_all_prior_history():
    history = [100.0, 120.0, 90.0, 110.0, 105.0, 95.0]
    expected = (700 - statistics.fmean(history)) / statistics.stdev(history)
    assert outflow_z_score(700, [95.0, 110.0], history) == pytest.approx(expected)
    assert outflow_z_score(700, [95.0, 110.0], history) > 3


def test_no_score_when_neither_baseline_has_enough_history():
    assert outflow_z_score(700, [1.0], [1.0, 2.0, 3.0, 4.0]) is None


def test_no_score_without_spread_and_no_fallback_past_a_full_recent_window():
    assert outflow_z_score(50, [15.49] * 5, [1.0, 2.0, 3.0, 4.0, 5.0, *[15.49] * 5]) is None


def test_min_history_is_configurable():
    assert outflow_z_score(10, [1.0, 2.0, 3.0], [1.0, 2.0, 3.0], min_history=3) == pytest.approx(8.0)


def test_uses_sample_standard_deviation_like_spark_stddev_samp():
    assert outflow_z_score(4, [0.0, 2.0], [0.0, 2.0], min_history=2) == pytest.approx((4 - 1) / 2 ** 0.5)
