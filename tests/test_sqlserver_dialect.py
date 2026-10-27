"""Pure (no-DB, no-pyodbc) tests for sqlserver_dialect.SqlServerDialect.

Every DB-calling method (get_connection, validate_table, boundaries,
discover_candidates, capture_watermark, segment_size_bytes, is_partitioned,
list_partitions) is operator / in-env integration only and is NOT exercised here.
Only the pure surface is covered: render_predicate, _format_literal, limited,
parse_owner_table, the snapshot overrides, and explain() against CANNED
SHOWPLAN_XML fed through a fake cursor (no live DB, no pyodbc).
"""

import datetime
from decimal import Decimal

import pytest

from sqlserver_dialect import SqlServerDialect
from source_dialect import Boundary, Candidate, PlanSummary


@pytest.fixture
def d():
    return SqlServerDialect()


# ---------------------------------------------------------------------------
# fake DBAPI objects (for explain only)
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def execute(self, sql, *params):
        self.executed.append(sql)
        return self

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _FakeCursor(self._rows)


def _conn_returning_xml(xml_text):
    return _FakeConn([(xml_text,)])


# ---------------------------------------------------------------------------
# canned SHOWPLAN_XML fragments
# ---------------------------------------------------------------------------

_NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"

_SHOWPLAN_CLUSTERED_SCAN = f"""<?xml version="1.0"?>
<ShowPlanXML xmlns="{_NS}" Version="1.539" Build="16.0.1000.6">
  <BatchSequence><Batch><Statements>
    <StmtSimple StatementText="SELECT * FROM dbo.Big" StatementType="SELECT" StatementEstRows="2000000">
      <QueryPlan>
        <RelOp NodeId="0" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="2000000">
          <IndexScan Ordered="0">
            <Object Database="[testdb]" Schema="[dbo]" Table="[Big]" Index="[PK_Big]" IndexKind="Clustered" />
          </IndexScan>
        </RelOp>
      </QueryPlan>
    </StmtSimple>
  </Statements></Batch></BatchSequence>
</ShowPlanXML>"""

_SHOWPLAN_INDEX_SEEK = f"""<?xml version="1.0"?>
<ShowPlanXML xmlns="{_NS}" Version="1.539" Build="16.0.1000.6">
  <BatchSequence><Batch><Statements>
    <StmtSimple StatementText="SELECT * FROM dbo.Big WHERE dt &gt;= '2024-01-01'" StatementType="SELECT" StatementEstRows="42">
      <QueryPlan>
        <RelOp NodeId="0" PhysicalOp="Index Seek" LogicalOp="Index Seek" EstimateRows="42">
          <IndexScan Ordered="1">
            <Object Database="[testdb]" Schema="[dbo]" Table="[Big]" Index="[IX_dt]" IndexKind="NonClustered" />
            <SeekPredicates><SeekPredicateNew><SeekKeys /></SeekPredicateNew></SeekPredicates>
          </IndexScan>
        </RelOp>
      </QueryPlan>
    </StmtSimple>
  </Statements></Batch></BatchSequence>
</ShowPlanXML>"""

# Nested-loops join: driving Index Seek (disjoint) + large inner Clustered Index Scan.
_SHOWPLAN_JOIN_AMPLIFY = f"""<?xml version="1.0"?>
<ShowPlanXML xmlns="{_NS}" Version="1.539" Build="16.0.1000.6">
  <BatchSequence><Batch><Statements>
    <StmtSimple StatementText="SELECT ... JOIN ..." StatementType="SELECT" StatementEstRows="100">
      <QueryPlan>
        <RelOp NodeId="0" PhysicalOp="Nested Loops" LogicalOp="Inner Join" EstimateRows="100">
          <NestedLoops>
            <RelOp NodeId="1" PhysicalOp="Index Seek" LogicalOp="Index Seek" EstimateRows="100">
              <IndexScan Ordered="1">
                <Object Database="[testdb]" Schema="[dbo]" Table="[Driving]" Index="[IX_id]" />
                <SeekPredicates><SeekPredicateNew><SeekKeys /></SeekPredicateNew></SeekPredicates>
              </IndexScan>
            </RelOp>
            <RelOp NodeId="2" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="5000000">
              <IndexScan Ordered="0">
                <Object Database="[testdb]" Schema="[dbo]" Table="[Lookup]" Index="[PK_Lookup]" IndexKind="Clustered" />
              </IndexScan>
            </RelOp>
          </NestedLoops>
        </RelOp>
      </QueryPlan>
    </StmtSimple>
  </Statements></Batch></BatchSequence>
</ShowPlanXML>"""


# ---------------------------------------------------------------------------
# instantiation + class attrs (import-safe, no pyodbc)
# ---------------------------------------------------------------------------


def test_instantiates_and_name():
    d = SqlServerDialect()
    assert d.name == "sqlserver"
    assert d.watermark_label == "SQL Server LSN"


# ---------------------------------------------------------------------------
# render_partition_line — $PARTITION function predicate
# ---------------------------------------------------------------------------
def test_render_partition_line_partition_function_predicate(d):
    spec = {"number": 3, "func": "pf_range", "col": "created_dt"}
    q = "SELECT * FROM dbo.Sales WHERE ~SPLIT~"
    line = d.render_partition_line(q, "dbo", "Sales", spec, "dbo.Tgt")
    assert (
        line
        == "SELECT * FROM dbo.Sales WHERE $PARTITION.[pf_range](created_dt) = 3|dbo.Tgt"
    )


def test_render_partition_line_alias_qualifies_column(d):
    spec = {"number": 1, "func": "pf", "col": "dt"}
    q = "SELECT * FROM dbo.S s WHERE ~SPLIT~"
    line = d.render_partition_line(q, "dbo", "S", spec, "dbo.Tgt", alias="s")
    assert line == "SELECT * FROM dbo.S s WHERE $PARTITION.[pf](s.dt) = 1|dbo.Tgt"


# ---------------------------------------------------------------------------
# render_predicate — physical (clustered key range)
# ---------------------------------------------------------------------------


def test_render_physical_key_range_no_alias(d):
    cand = Candidate(kind="physical", key="ID", label="ID")
    b = Boundary(lo=1000, hi=2000, label="ID")
    assert d.render_predicate(cand, b) == "ID >= 1000 AND ID < 2000"


def test_render_physical_key_range_with_alias(d):
    cand = Candidate(kind="physical", key="ID", label="ID")
    b = Boundary(lo=1000, hi=2000, label="ID")
    assert d.render_predicate(cand, b, alias="t") == "t.ID >= 1000 AND t.ID < 2000"


def test_render_physical_open_below(d):
    cand = Candidate(kind="physical", key="ID", label="ID")
    b = Boundary(lo=None, hi=1000, label="ID")
    assert d.render_predicate(cand, b) == "ID < 1000"


def test_render_physical_open_above(d):
    cand = Candidate(kind="physical", key="ID", label="ID")
    b = Boundary(lo=2000, hi=None, label="ID")
    assert d.render_predicate(cand, b) == "ID >= 2000"


def test_render_physical_all_non_null(d):
    cand = Candidate(kind="physical", key="ID", label="ID")
    b = Boundary(lo=None, hi=None, label="ID")
    assert d.render_predicate(cand, b) == "ID IS NOT NULL"


# ---------------------------------------------------------------------------
# render_predicate — column (half-open + IS NULL cover)
# ---------------------------------------------------------------------------


def test_render_column_int_no_alias(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=1000, hi=2000, label="ID")
    assert d.render_predicate(cand, b) == "ID >= 1000 AND ID < 2000"


def test_render_column_alias_qualifies(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=1000, hi=2000, label="ID")
    assert d.render_predicate(cand, b, alias="s") == "s.ID >= 1000 AND s.ID < 2000"


def test_render_column_alias_skipped_when_key_already_dotted(d):
    cand = Candidate(kind="column", key="s.CREATED_DT", label="s.CREATED_DT")
    b = Boundary(lo=1, hi=2)
    assert (
        d.render_predicate(cand, b, alias="x")
        == "s.CREATED_DT >= 1 AND s.CREATED_DT < 2"
    )


def test_render_column_open_below(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=None, hi=1000, label="ID")
    assert d.render_predicate(cand, b) == "ID < 1000"


def test_render_column_open_above(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=2000, hi=None, label="ID")
    assert d.render_predicate(cand, b) == "ID >= 2000"


def test_render_column_is_null(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(is_null=True, label="ID")
    assert d.render_predicate(cand, b) == "ID IS NULL"


def test_render_column_is_null_alias_qualifies(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(is_null=True, label="ID")
    assert d.render_predicate(cand, b, alias="s") == "s.ID IS NULL"


def test_render_column_datetime_range(d):
    cand = Candidate(kind="column", key="CREATED_DT", label="CREATED_DT")
    b = Boundary(
        lo=datetime.datetime(2024, 1, 1, 0, 0, 0),
        hi=datetime.datetime(2024, 2, 1, 12, 30, 45),
        label="CREATED_DT",
    )
    out = d.render_predicate(cand, b, alias="s")
    assert out == (
        "s.CREATED_DT >= '2024-01-01T00:00:00' "
        "AND s.CREATED_DT < '2024-02-01T12:30:45'"
    )


def test_render_unknown_kind_raises(d):
    cand = Candidate(kind="hash", key="X", label="X")
    with pytest.raises(ValueError):
        d.render_predicate(cand, Boundary(lo=1, hi=2))


# ---------------------------------------------------------------------------
# _format_literal (pure) — T-SQL literals
# ---------------------------------------------------------------------------


def test_format_literal_int(d):
    assert d._format_literal(42) == "42"


def test_format_literal_float(d):
    assert d._format_literal(1.5) == "1.5"


def test_format_literal_decimal(d):
    assert d._format_literal(Decimal("3.14")) == "3.14"


def test_format_literal_str_unicode_doubles_quotes(d):
    assert d._format_literal("O'Brien") == "N'O''Brien'"


def test_format_literal_date(d):
    assert d._format_literal(datetime.date(2024, 3, 4)) == "'2024-03-04'"


def test_format_literal_datetime_no_subsecond(d):
    v = datetime.datetime(2024, 3, 4, 5, 6, 7)
    assert d._format_literal(v) == "'2024-03-04T05:06:07'"


def test_format_literal_datetime_microseconds_to_millis(d):
    v = datetime.datetime(2024, 3, 4, 5, 6, 7, 123456)
    # microseconds are truncated to milliseconds (datetime precision)
    assert d._format_literal(v) == "'2024-03-04T05:06:07.123'"


# ---------------------------------------------------------------------------
# limited (pure) — TOP + aliased subquery
# ---------------------------------------------------------------------------


def test_limited_wraps_aliased_subquery(d):
    assert (
        d.limited("SELECT * FROM t", 100)
        == "SELECT TOP (100) * FROM (SELECT * FROM t) _probe"
    )


def test_limited_coerces_int(d):
    assert (
        d.limited("SELECT a FROM t", "50")
        == "SELECT TOP (50) * FROM (SELECT a FROM t) _probe"
    )


# ---------------------------------------------------------------------------
# explain — canned SHOWPLAN_XML via fake cursor (no DB, no pyodbc)
# ---------------------------------------------------------------------------


def test_explain_clustered_index_scan_not_disjoint(d):
    ps = d.explain(
        _conn_returning_xml(_SHOWPLAN_CLUSTERED_SCAN), "SELECT * FROM dbo.Big"
    )
    assert isinstance(ps, PlanSummary)
    assert ps.access_path == "Clustered Index Scan"
    assert ps.is_disjoint is False
    # a >1e6-row scan IS flagged as join-amplification
    assert ps.full_scans == ["Big"]


def test_explain_index_seek_disjoint(d):
    ps = d.explain(_conn_returning_xml(_SHOWPLAN_INDEX_SEEK), "SELECT ...")
    assert ps.access_path == "Index Seek"
    assert ps.is_disjoint is True
    assert ps.full_scans == []


def test_explain_join_driving_seek_with_amplification(d):
    ps = d.explain(_conn_returning_xml(_SHOWPLAN_JOIN_AMPLIFY), "SELECT ...")
    # driving access path is the first table access in document order
    assert ps.access_path == "Index Seek"
    assert ps.is_disjoint is True
    assert ps.full_scans == ["Lookup"]


def test_explain_malformed_xml_degrades_to_generic(d):
    ps = d.explain(_conn_returning_xml("<not valid xml"), "SELECT ...")
    assert isinstance(ps, PlanSummary)
    assert ps.is_disjoint is False
    assert ps.full_scans == []
    assert "showplan" in ps.access_path.lower()


def test_explain_cursor_error_degrades_to_generic(d):
    class _BoomConn:
        def cursor(self):
            raise RuntimeError("driver exploded")

    ps = d.explain(_BoomConn(), "SELECT ...")
    assert isinstance(ps, PlanSummary)
    assert ps.is_disjoint is False
    assert ps.full_scans == []


# ---------------------------------------------------------------------------
# parse_owner_table — schema.table, default dbo (case-preserving)
# ---------------------------------------------------------------------------


def test_parse_owner_table_schema_qualified(d):
    assert d.parse_owner_table("Sales.Orders") == ("Sales", "Orders")


def test_parse_owner_table_defaults_dbo(d):
    assert d.parse_owner_table("Orders") == ("dbo", "Orders")


# ---------------------------------------------------------------------------
# engine-aware reconcile — SQL Server has no point-in-time snapshot
# ---------------------------------------------------------------------------


def test_snapshot_enable_sql_is_none(d):
    assert d.snapshot_enable_sql({"label": "SQL Server LSN", "value": "0x00"}) is None
    assert d.snapshot_enable_sql(None) is None


def test_snapshot_disable_sql_is_none(d):
    assert d.snapshot_disable_sql() is None


def test_is_snapshot_lost_always_false(d):
    assert d.is_snapshot_lost(Exception("anything")) is False
