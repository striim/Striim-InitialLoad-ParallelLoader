import pytest
import query_split as qs

JOIN = (
    "SELECT s.A FROM SALES.ORDERS s "
    "JOIN SALES.ORDER_LINES r ON r.T = s.T "
    "WHERE s.REGION = 'WEST' AND ~SPLIT~"
)


def test_detect_alias_in_join():
    assert qs.detect_alias(JOIN, "SALES", "ORDERS") == "s"


def test_detect_alias_absent_returns_none():
    q = "SELECT * FROM SALES.ORDERS WHERE ~SPLIT~"
    assert qs.detect_alias(q, "SALES", "ORDERS") is None


def test_detect_alias_skips_keyword():
    q = "SELECT * FROM SALES.T WHERE ~SPLIT~"
    assert qs.detect_alias(q, "SALES", "T") is None


def test_render_rowid_replaces_split_token():
    line = qs.render_rowid_line(JOIN, "s", "AAAlo", "AAAhi", "SALES.TGT")
    assert "s.ROWID BETWEEN 'AAAlo' AND 'AAAhi'" in line
    assert "~SPLIT~" not in line
    assert line.endswith("|SALES.TGT")


def test_render_rowid_append_when_no_token_and_where():
    q = "SELECT * FROM SALES.T t WHERE t.X = 1"
    line = qs.render_rowid_line(q, "t", "lo", "hi", "SALES.TGT")
    assert (
        line
        == "SELECT * FROM SALES.T t WHERE t.X = 1 AND t.ROWID BETWEEN 'lo' AND 'hi'|SALES.TGT"
    )


def test_render_rowid_append_when_no_token_no_where():
    q = "SELECT * FROM SALES.T t"
    line = qs.render_rowid_line(q, "t", "lo", "hi", "SALES.TGT")
    assert line == "SELECT * FROM SALES.T t WHERE t.ROWID BETWEEN 'lo' AND 'hi'|SALES.TGT"


def test_render_rowid_join_without_alias_raises():
    with pytest.raises(qs.SplitError):
        qs.render_rowid_line(
            JOIN.replace("~SPLIT~", "1=1"), None, "lo", "hi", "SALES.TGT"
        )


def test_render_partition_rewrites_from_and_neutralizes_token():
    line = qs.render_partition_line(JOIN, "SALES", "ORDERS", "P1", "SALES.TGT")
    assert "SALES.ORDERS PARTITION (P1) s" in line
    assert "AND 1=1" in line
    assert "~SPLIT~" not in line


def test_render_partition_subpartition_keyword():
    q = "SELECT * FROM SALES.T t WHERE ~SPLIT~"
    line = qs.render_partition_line(q, "SALES", "T", "SP3", "SALES.TGT", sub=True)
    assert "SALES.T SUBPARTITION (SP3) t" in line


def test_render_partition_table_absent_raises():
    with pytest.raises(qs.SplitError):
        qs.render_partition_line("SELECT 1 FROM DUAL", "SALES", "T", "P1", "SALES.TGT")


def test_render_child_table_line_swaps_table_ref():
    q = "SELECT * FROM PUB.MEAS m WHERE ~SPLIT~"
    line = qs.render_child_table_line(q, "PUB", "MEAS", "MEAS_2024", "PUB.TGT")
    assert line == "SELECT * FROM PUB.MEAS_2024 m WHERE 1=1|PUB.TGT"


def test_render_child_table_line_preserves_alias_and_neutralizes_token():
    line = qs.render_child_table_line(
        JOIN, "SALES", "ORDERS", "PART_3", "SALES.TGT"
    )
    assert "SALES.PART_3 s" in line  # alias 's' preserved; child relation swapped in
    assert "AND 1=1" in line
    assert "~SPLIT~" not in line


def test_render_child_table_line_table_absent_raises():
    with pytest.raises(qs.SplitError):
        qs.render_child_table_line("SELECT 1 FROM DUAL", "SALES", "T", "T_P1", "SALES.TGT")


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
    q = "SELECT * FROM SALES.T WHERE ~SPLIT~"
    result = qs.inject_predicate(q, "X = 1")
    assert result == "SELECT * FROM SALES.T WHERE X = 1"
    assert "~SPLIT~" not in result


def test_inject_predicate_appends_and_when_where_exists():
    q = "SELECT * FROM SALES.T WHERE Y = 2"
    result = qs.inject_predicate(q, "X = 1")
    assert result == "SELECT * FROM SALES.T WHERE Y = 2 AND X = 1"


def test_inject_predicate_appends_where_when_no_token_no_where():
    q = "SELECT * FROM SALES.T"
    result = qs.inject_predicate(q, "X = 1")
    assert result == "SELECT * FROM SALES.T WHERE X = 1"


def test_inject_predicate_strips_trailing_semicolon_on_token_path():
    q = "SELECT * FROM SALES.T WHERE ~SPLIT~;"
    result = qs.inject_predicate(q, "X = 1")
    assert result == "SELECT * FROM SALES.T WHERE X = 1"
    assert not result.rstrip().endswith(";")
    assert "~SPLIT~" not in result


# --- render_column_range_line ---


def test_render_column_range_line_half_open_default():
    q = "SELECT * FROM SALES.T t WHERE ~SPLIT~"
    line = qs.render_column_range_line(q, "X", "10", "20", "SALES.TGT")
    assert "X >= 10 AND X < 20" in line
    assert line.endswith("|SALES.TGT")
    assert "~SPLIT~" not in line


def test_render_column_range_line_inclusive_hi():
    q = "SELECT * FROM SALES.T t WHERE ~SPLIT~"
    line = qs.render_column_range_line(q, "X", "10", "20", "SALES.TGT", inclusive_hi=True)
    assert "X >= 10 AND X <= 20" in line
    assert line.endswith("|SALES.TGT")


def test_render_column_range_line_values_pass_through_verbatim():
    lo = "TO_DATE('2020-01-01','YYYY-MM-DD')"
    hi = "TO_DATE('2021-01-01','YYYY-MM-DD')"
    q = "SELECT * FROM SALES.T t WHERE ~SPLIT~"
    line = qs.render_column_range_line(q, "DT", lo, hi, "SALES.TGT")
    assert lo in line
    assert hi in line


def test_render_column_range_line_alias_qualified_col():
    q = "SELECT * FROM SALES.T s WHERE ~SPLIT~"
    line = qs.render_column_range_line(q, "s.CREATED_DT", "100", "200", "SALES.TGT")
    assert "s.CREATED_DT >= 100 AND s.CREATED_DT < 200" in line
    assert line.endswith("|SALES.TGT")


# --- defensive input validation (validate_identifier / validate_target) ---


def test_validate_identifier_accepts_bare_ident():
    assert qs.validate_identifier("t", "alias") == "t"
    assert qs.validate_identifier("ORDERS$#", "alias") == "ORDERS$#"


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
    assert qs.validate_target("SALES.TGT") == "SALES.TGT"


@pytest.mark.parametrize("bad", ["", "   ", "SALES|X", "|", "a|b|c"])
def test_validate_target_rejects_empty_or_piped(bad):
    with pytest.raises(qs.SplitError):
        qs.validate_target(bad)


# --- inject_hint (Oracle PARALLEL etc.) ---


def test_inject_hint_inserts_after_leading_select():
    q = "SELECT * FROM SALES.T WHERE x = 1"
    assert qs.inject_hint(q, "PARALLEL(4)") == "SELECT /*+ PARALLEL(4) */ * FROM SALES.T WHERE x = 1"


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
