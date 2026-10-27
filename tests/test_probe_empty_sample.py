"""Pure tests: a candidate whose sampled slice returned no rows is never ranked.

On a skewed key the first slice can be empty (e.g. ``customer_id < 1`` when 40% of
rows share the lowest value). Its timing is 0 rows / 0 bytes, which says nothing
about throughput, so it must not be a "good" candidate or the recommendation.
"""
from probe import CandidateResult, _verdict, empty_sample_warnings, rank_candidates
from source_dialect import Candidate, PlanSummary, TimingResult


def _result(label, kind, rows, nbytes, *, disjoint=True, full_scans=None):
    cand = Candidate(kind=kind, key=None if kind == "physical" else label, label=label)
    plan = PlanSummary(access_path="Index Scan", is_disjoint=disjoint,
                       full_scans=list(full_scans or []))
    return CandidateResult(candidate=cand, plan=plan,
                           timing=TimingResult(rows=rows, bytes=nbytes, seconds=0.01))


def test_empty_sample_verdict():
    assert _verdict(_result("customer_id", "column", 0, 0)) == "empty-sample"


def test_amplifying_verdict_wins_over_empty_sample():
    r = _result("customer_id", "column", 0, 0, full_scans=["order_lines"])
    assert _verdict(r) == "AMPLIFYING"


def test_empty_sample_is_ranked_below_an_amplifying_candidate():
    # 3b shape: every timed candidate is AMPLIFYING except an empty skewed slice.
    empty = _result("customer_id", "column", 0, 0)
    amp = _result("ctid", "physical", 5000, 2_000_000, disjoint=False,
                  full_scans=["order_lines"])
    assert rank_candidates([empty, amp])[0].candidate.label == "ctid"


def test_empty_sample_never_beats_a_good_candidate():
    good = _result("ctid", "physical", 1000, 100_000)
    empty = _result("customer_id", "column", 0, 0)
    assert rank_candidates([empty, good])[0].candidate.label == "ctid"


def test_empty_sample_warning_names_the_candidate():
    ws = empty_sample_warnings([_result("customer_id", "column", 0, 0),
                                _result("ctid", "physical", 10, 100)])
    assert len(ws) == 1
    assert "customer_id" in ws[0] and "no rows" in ws[0]
