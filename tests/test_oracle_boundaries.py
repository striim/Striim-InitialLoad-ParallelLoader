import pytest
import oracle_boundaries as ob


def test_parse_owner_table_ok():
    assert ob.parse_owner_table("sales.orders") == ("SALES", "ORDERS")


def test_parse_owner_table_bad():
    with pytest.raises(ob.BoundaryError):
        ob.parse_owner_table("justtable")


def test_validate_ident_rejects_injection():
    with pytest.raises(ob.BoundaryError):
        ob.validate_ident("T; DROP TABLE X")


def test_is_priv_error_detects_codes():
    assert ob._is_priv_error(Exception("ORA-01031: insufficient privileges"))
    assert ob._is_priv_error(Exception("ORA-00942: table or view does not exist"))
    assert not ob._is_priv_error(Exception("ORA-12541: no listener"))


def test_rowid_ranges_falls_back_to_third_method(monkeypatch):
    calls = []

    def m1(conn, o, t, n):
        calls.append("m1")
        raise Exception("ORA-01031: insufficient privileges")

    def m2(conn, o, t, n):
        calls.append("m2")
        raise Exception("ORA-00942: table or view does not exist")

    def m3(conn, o, t, n):
        calls.append("m3")
        return [("AAAlo", "AAAhi")]

    monkeypatch.setattr(ob, "_ranges_dbms_parallel", m1)
    monkeypatch.setattr(ob, "_ranges_dba_extents", m2)
    monkeypatch.setattr(ob, "_ranges_ntile", m3)

    assert ob.rowid_ranges(None, "SALES", "T", 4) == [("AAAlo", "AAAhi")]
    assert calls == ["m1", "m2", "m3"]


def test_rowid_ranges_reraises_non_priv_error(monkeypatch):
    def boom(conn, o, t, n):
        raise Exception("ORA-12541: TNS no listener")

    monkeypatch.setattr(ob, "_ranges_dbms_parallel", boom)
    with pytest.raises(Exception) as e:
        ob.rowid_ranges(None, "SALES", "T", 4)
    assert "ORA-12541" in str(e.value)


# ---------------------------------------------------------------------------
# Pure unit tests: _is_splittable_type + _rank_split_columns (no DB needed)
# ---------------------------------------------------------------------------


def test_is_splittable_type_number():
    assert ob._is_splittable_type("NUMBER") is True


def test_is_splittable_type_float():
    assert ob._is_splittable_type("FLOAT") is True


def test_is_splittable_type_date():
    assert ob._is_splittable_type("DATE") is True


def test_is_splittable_type_timestamp_with_tz():
    assert ob._is_splittable_type("TIMESTAMP(6) WITH TIME ZONE") is True


def test_is_splittable_type_timestamp_plain():
    assert ob._is_splittable_type("TIMESTAMP") is True


def test_is_splittable_type_timestamp_6():
    assert ob._is_splittable_type("TIMESTAMP(6)") is True


def test_is_splittable_type_varchar2_rejected():
    assert ob._is_splittable_type("VARCHAR2") is False


def test_is_splittable_type_clob_rejected():
    assert ob._is_splittable_type("CLOB") is False


def test_is_splittable_type_none_rejected():
    assert ob._is_splittable_type(None) is False


def _row(column_name, data_type, num_distinct, indexed):
    return {
        "column_name": column_name,
        "data_type": data_type,
        "num_distinct": num_distinct,
        "indexed": indexed,
    }


def test_rank_split_columns_indexed_beats_nonindexed():
    rows = [
        _row("AMOUNT", "NUMBER", 1000, 0),
        _row("TXN_DATE", "DATE", 500, 1),
    ]
    result = ob._rank_split_columns(rows)
    # TXN_DATE is indexed → ranks first despite lower NDV
    assert result == ["TXN_DATE", "AMOUNT"]


def test_rank_split_columns_high_ndv_beats_low_ndv_same_index():
    rows = [
        _row("LOW_COL", "NUMBER", 10, 0),
        _row("HIGH_COL", "NUMBER", 9999, 0),
    ]
    result = ob._rank_split_columns(rows)
    assert result == ["HIGH_COL", "LOW_COL"]


def test_rank_split_columns_drops_ndv_lte_1():
    rows = [
        _row("FLAG", "NUMBER", 1, 1),  # ndv == 1 → dropped even if indexed
        _row("ZERO", "NUMBER", 0, 1),  # ndv == 0 → dropped
        _row("GOOD", "NUMBER", 100, 0),
    ]
    result = ob._rank_split_columns(rows)
    assert result == ["GOOD"]


def test_rank_split_columns_drops_varchar2():
    rows = [
        _row("NAME", "VARCHAR2", 50000, 1),  # high NDV + indexed but wrong type
        _row("ID", "NUMBER", 100, 0),
    ]
    result = ob._rank_split_columns(rows)
    assert result == ["ID"]


def test_rank_split_columns_keeps_timestamp6():
    rows = [
        _row("CREATED_AT", "TIMESTAMP(6)", 200, 0),
    ]
    result = ob._rank_split_columns(rows)
    assert result == ["CREATED_AT"]


def test_rank_split_columns_empty_input():
    assert ob._rank_split_columns([]) == []


def test_rank_split_columns_all_filtered():
    rows = [
        _row("NAME", "VARCHAR2", 99999, 1),
        _row("DESC", "CLOB", 99999, 1),
    ]
    assert ob._rank_split_columns(rows) == []


def test_rank_split_columns_tiebreak_by_ndv():
    # Both indexed with same index value — higher NDV wins
    rows = [
        _row("A", "NUMBER", 500, 1),
        _row("B", "DATE", 5000, 1),
    ]
    result = ob._rank_split_columns(rows)
    assert result == ["B", "A"]


# ---------------------------------------------------------------------------
# Pure unit tests: slices_from_cuts (no DB needed) — the gap-free cover invariant
# ---------------------------------------------------------------------------


def test_slices_from_cuts_empty_is_single_all_slice():
    assert ob.slices_from_cuts([]) == [(None, None)]


def test_slices_from_cuts_three_cuts():
    assert ob.slices_from_cuts([10, 20, 30]) == [
        (None, 10),
        (10, 20),
        (20, 30),
        (30, None),
    ]


def test_slices_from_cuts_single_cut():
    assert ob.slices_from_cuts([5]) == [(None, 5), (5, None)]


def test_slices_from_cuts_is_gap_free_and_contiguous():
    cuts = [1, 4, 9, 16]
    out = ob.slices_from_cuts(cuts)
    # open ends present
    assert out[0][0] is None
    assert out[-1][1] is None
    # each slice's hi equals the next slice's lo -> no gaps, no overlaps
    for (a_lo, a_hi), (b_lo, b_hi) in zip(out, out[1:]):
        assert a_hi == b_lo
    # internal cut points exactly reconstruct the cuts
    internal = [hi for _lo, hi in out[:-1]]
    assert internal == cuts


def test_slices_from_cuts_accepts_iterables():
    assert ob.slices_from_cuts(iter([7, 8])) == [(None, 7), (7, 8), (8, None)]


# ---------------------------------------------------------------------------
# Integration tests for discover_split_columns, segment_size_bytes,
# column_range_boundaries require a live Oracle connection and cannot be
# exercised in this environment. Run them against the operator's Oracle DB.
# ---------------------------------------------------------------------------
