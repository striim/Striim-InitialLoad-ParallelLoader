import pytest
import query_split as qs

JOIN = (
    "SELECT s.A FROM PAY.CM_FB_SUBMISSION s "
    "JOIN PAY.CM_FB_SUBMSN_RESP r ON r.T = s.T "
    "WHERE s.ENTITY_TYPE = 'CASE' AND ~SPLIT~"
)


def test_detect_alias_in_join():
    assert qs.detect_alias(JOIN, "PAY", "CM_FB_SUBMISSION") == "s"


def test_detect_alias_absent_returns_none():
    q = "SELECT * FROM PAY.CM_FB_SUBMISSION WHERE ~SPLIT~"
    assert qs.detect_alias(q, "PAY", "CM_FB_SUBMISSION") is None


def test_detect_alias_skips_keyword():
    q = "SELECT * FROM PAY.T WHERE ~SPLIT~"
    assert qs.detect_alias(q, "PAY", "T") is None


def test_render_rowid_replaces_split_token():
    line = qs.render_rowid_line(JOIN, "s", "AAAlo", "AAAhi", "PAY.TGT")
    assert "s.ROWID BETWEEN 'AAAlo' AND 'AAAhi'" in line
    assert "~SPLIT~" not in line
    assert line.endswith("|PAY.TGT")


def test_render_rowid_append_when_no_token_and_where():
    q = "SELECT * FROM PAY.T t WHERE t.X = 1"
    line = qs.render_rowid_line(q, "t", "lo", "hi", "PAY.TGT")
    assert (
        line
        == "SELECT * FROM PAY.T t WHERE t.X = 1 AND t.ROWID BETWEEN 'lo' AND 'hi'|PAY.TGT"
    )


def test_render_rowid_append_when_no_token_no_where():
    q = "SELECT * FROM PAY.T t"
    line = qs.render_rowid_line(q, "t", "lo", "hi", "PAY.TGT")
    assert line == "SELECT * FROM PAY.T t WHERE t.ROWID BETWEEN 'lo' AND 'hi'|PAY.TGT"


def test_render_rowid_join_without_alias_raises():
    with pytest.raises(qs.SplitError):
        qs.render_rowid_line(
            JOIN.replace("~SPLIT~", "1=1"), None, "lo", "hi", "PAY.TGT"
        )


def test_render_partition_rewrites_from_and_neutralizes_token():
    line = qs.render_partition_line(JOIN, "PAY", "CM_FB_SUBMISSION", "P1", "PAY.TGT")
    assert "PAY.CM_FB_SUBMISSION PARTITION (P1) s" in line
    assert "AND 1=1" in line
    assert "~SPLIT~" not in line


def test_render_partition_subpartition_keyword():
    q = "SELECT * FROM PAY.T t WHERE ~SPLIT~"
    line = qs.render_partition_line(q, "PAY", "T", "SP3", "PAY.TGT", sub=True)
    assert "PAY.T SUBPARTITION (SP3) t" in line


def test_render_partition_table_absent_raises():
    with pytest.raises(qs.SplitError):
        qs.render_partition_line("SELECT 1 FROM DUAL", "PAY", "T", "P1", "PAY.TGT")


def test_render_child_table_line_swaps_table_ref():
    q = "SELECT * FROM PUB.MEAS m WHERE ~SPLIT~"
    line = qs.render_child_table_line(q, "PUB", "MEAS", "MEAS_2024", "PUB.TGT")
    assert line == "SELECT * FROM PUB.MEAS_2024 m WHERE 1=1|PUB.TGT"


def test_render_child_table_line_preserves_alias_and_neutralizes_token():
    line = qs.render_child_table_line(
        JOIN, "PAY", "CM_FB_SUBMISSION", "PART_3", "PAY.TGT"
    )
    assert "PAY.PART_3 s" in line  # alias 's' preserved; child relation swapped in
    assert "AND 1=1" in line
    assert "~SPLIT~" not in line


def test_render_child_table_line_table_absent_raises():
    with pytest.raises(qs.SplitError):
        qs.render_child_table_line("SELECT 1 FROM DUAL", "PAY", "T", "T_P1", "PAY.TGT")


def test_coalesce_merges_overlapping():
    assert qs.coalesce_ranges([("a", "c"), ("c", "e"), ("g", "h")]) == [
        ("a", "e"),
        ("g", "h"),
    ]


def test_coalesce_preserves_db_order_disjoint():
    # DB-ordered disjoint ranges are returned unchanged (no merge).
    inp = [("AAA", "AAF"), ("AAG", "AAM"), ("AAN", "AAZ")]
    assert qs.coalesce_ranges(inp) == inp


def test_coalesce_does_not_merge_across_misordered_neighbor():
    # A backward-jumping range must NOT be silently merged/absorbed (no data loss).
    assert qs.coalesce_ranges([("ABA", "ABZ"), ("AAA", "AAM")]) == [
        ("ABA", "ABZ"),
        ("AAA", "AAM"),
    ]


def test_format_lines_trailing_newline():
    assert qs.format_lines(["a|b", "c|d"]) == "a|b\nc|d\n"


# --- inject_predicate (public) ---


def test_inject_predicate_replaces_split_token():
    q = "SELECT * FROM PAY.T WHERE ~SPLIT~"
    result = qs.inject_predicate(q, "X = 1")
    assert result == "SELECT * FROM PAY.T WHERE X = 1"
    assert "~SPLIT~" not in result


def test_inject_predicate_appends_and_when_where_exists():
    q = "SELECT * FROM PAY.T WHERE Y = 2"
    result = qs.inject_predicate(q, "X = 1")
    assert result == "SELECT * FROM PAY.T WHERE Y = 2 AND X = 1"


def test_inject_predicate_appends_where_when_no_token_no_where():
    q = "SELECT * FROM PAY.T"
    result = qs.inject_predicate(q, "X = 1")
    assert result == "SELECT * FROM PAY.T WHERE X = 1"


def test_inject_predicate_strips_trailing_semicolon_on_token_path():
    q = "SELECT * FROM PAY.T WHERE ~SPLIT~;"
    result = qs.inject_predicate(q, "X = 1")
    assert result == "SELECT * FROM PAY.T WHERE X = 1"
    assert not result.rstrip().endswith(";")
    assert "~SPLIT~" not in result


# --- render_column_range_line ---


def test_render_column_range_line_half_open_default():
    q = "SELECT * FROM PAY.T t WHERE ~SPLIT~"
    line = qs.render_column_range_line(q, "X", "10", "20", "PAY.TGT")
    assert "X >= 10 AND X < 20" in line
    assert line.endswith("|PAY.TGT")
    assert "~SPLIT~" not in line


def test_render_column_range_line_inclusive_hi():
    q = "SELECT * FROM PAY.T t WHERE ~SPLIT~"
    line = qs.render_column_range_line(q, "X", "10", "20", "PAY.TGT", inclusive_hi=True)
    assert "X >= 10 AND X <= 20" in line
    assert line.endswith("|PAY.TGT")


def test_render_column_range_line_values_pass_through_verbatim():
    lo = "TO_DATE('2020-01-01','YYYY-MM-DD')"
    hi = "TO_DATE('2021-01-01','YYYY-MM-DD')"
    q = "SELECT * FROM PAY.T t WHERE ~SPLIT~"
    line = qs.render_column_range_line(q, "DT", lo, hi, "PAY.TGT")
    assert lo in line
    assert hi in line


def test_render_column_range_line_alias_qualified_col():
    q = "SELECT * FROM PAY.T s WHERE ~SPLIT~"
    line = qs.render_column_range_line(q, "s.CREATED_DT", "100", "200", "PAY.TGT")
    assert "s.CREATED_DT >= 100 AND s.CREATED_DT < 200" in line
    assert line.endswith("|PAY.TGT")


# --- defensive input validation (validate_identifier / validate_target) ---


def test_validate_identifier_accepts_bare_ident():
    assert qs.validate_identifier("t", "alias") == "t"
    assert qs.validate_identifier("CM_CASES$#", "alias") == "CM_CASES$#"


def test_validate_identifier_passes_none_and_empty_through():
    assert qs.validate_identifier(None, "alias") is None
    assert qs.validate_identifier("", "alias") == ""


import pytest


@pytest.mark.parametrize(
    "bad", ["x WHERE 1=1--", "a b", "a';DROP TABLE t;--", "a|b", "a.b"]
)
def test_validate_identifier_rejects_injection_and_typos(bad):
    with pytest.raises(qs.SplitError):
        qs.validate_identifier(bad, "alias")


def test_validate_identifier_allows_one_dot_when_qualified():
    assert qs.validate_identifier("s.CREATED_DT", "column", allow_qualified=True) == (
        "s.CREATED_DT"
    )
    with pytest.raises(qs.SplitError):
        qs.validate_identifier("s.a.b", "column", allow_qualified=True)


def test_validate_target_accepts_plain_target():
    assert qs.validate_target("PAY.TGT") == "PAY.TGT"


@pytest.mark.parametrize("bad", ["", "   ", "PAY|X", "|", "a|b|c"])
def test_validate_target_rejects_empty_or_piped(bad):
    with pytest.raises(qs.SplitError):
        qs.validate_target(bad)


# --- inject_hint (Oracle PARALLEL etc.) ---


def test_inject_hint_inserts_after_leading_select():
    q = "SELECT * FROM PAY.T WHERE x = 1"
    assert qs.inject_hint(q, "PARALLEL(4)") == "SELECT /*+ PARALLEL(4) */ * FROM PAY.T WHERE x = 1"


def test_inject_hint_preserves_lowercase_keyword():
    q = "select a, b from t"
    assert qs.inject_hint(q, "PARALLEL(4)") == "select /*+ PARALLEL(4) */ a, b from t"


def test_inject_hint_preserves_leading_whitespace():
    q = "\n  SELECT * FROM T"
    assert qs.inject_hint(q, "PARALLEL(4)") == "\n  SELECT /*+ PARALLEL(4) */ * FROM T"


def test_inject_hint_merges_into_existing_hint_block():
    q = "SELECT /*+ FULL(t) */ * FROM T t"
    assert qs.inject_hint(q, "PARALLEL(4)") == "SELECT /*+ FULL(t) PARALLEL(4) */ * FROM T t"


def test_inject_hint_does_not_duplicate_existing_parallel():
    q = "SELECT /*+ PARALLEL(2) */ * FROM T"
    assert qs.inject_hint(q, "PARALLEL(4)") == q


def test_inject_hint_empty_or_none_returns_query_unchanged():
    q = "SELECT * FROM T"
    assert qs.inject_hint(q, "") == q
    assert qs.inject_hint(q, None) == q


def test_inject_hint_non_select_raises():
    with pytest.raises(qs.SplitError):
        qs.inject_hint("UPDATE T SET x = 1", "PARALLEL(4)")
