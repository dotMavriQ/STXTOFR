from app.normalization.merge import score_candidate


def test_score_candidate_for_same_name_nearby_rows() -> None:
    left = {"id": 1, "facility_name": "Circle K Arlandastad", "latitude": 59.6, "longitude": 17.8}
    right = {"id": 2, "facility_name": "Circle K Arlandastad", "latitude": 59.61, "longitude": 17.81}
    candidate = score_candidate(left, right)
    assert candidate is not None
    assert candidate.score > 0.6

