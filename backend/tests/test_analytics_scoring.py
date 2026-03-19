from backend.analytics import _calculate_final_score, _post_process_row


def test_calculate_final_score_stays_positive_for_realistic_period_metrics():
    score = _calculate_final_score(
        avg_score=83.0,
        avg_time_min=2.9,
        missed_days=1,
    )

    assert score == 81.05


def test_post_process_row_builds_non_zero_final_score_without_collapsing_on_missed_sentences():
    processed = _post_process_row(
        {
            "translation_attempts": 36,
            "covered_sentences": 28,
            "successful_translations": 20,
            "total_time_min": 81.2,
            "assigned_sentences": 31,
            "avg_score": 83.0,
            "missed_days": 1,
        }
    )

    assert processed["completion_rate"] == 90.3
    assert processed["success_rate"] == 71.4
    assert processed["missed_sentences"] == 3
    assert processed["final_score"] > 0


def test_calculate_final_score_can_go_negative_when_penalties_dominate():
    score = _calculate_final_score(
        avg_score=48.0,
        avg_time_min=6.0,
        missed_days=100,
    )

    assert score < 0
    assert score == -5.0
