"""Pure query-rewriting + queryfile emission for the splitter. No DB / oracledb imports."""

import re

SPLIT_TOKEN = "~SPLIT~"
_IDENT = r"[A-Za-z0-9_$#]+"
_ALIAS_KEYWORDS = (
    "PARTITION",
    "SUBPARTITION",
    "WHERE",
    "ON",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "CROSS",
    "GROUP",
    "ORDER",
    "HAVING",
    "CONNECT",
    "START",
    "UNION",
    "MINUS",
    "INTERSECT",
)


class SplitError(Exception):
    pass


_BARE_IDENT_RE = re.compile(r"^[A-Za-z0-9_$#]+$")
_QUALIFIED_IDENT_RE = re.compile(r"^[A-Za-z0-9_$#]+(\.[A-Za-z0-9_$#]+)?$")


def validate_identifier(value, kind="identifier", allow_qualified=False):
    """Reject a user-supplied identifier (alias, split column) that is not a bare SQL
    identifier before it is string-interpolated into executed SQL. Blocks injection
    (spaces, quotes, ``;``, ``|``, ``--``) and obvious typos with a clear error instead
    of shipping a broken/hostile predicate to the source DB.

    None / "" pass through unchanged (means "not supplied"). ``allow_qualified`` permits
    one ``owner.col`` dot (a column may arrive already alias-qualified)."""
    if value is None or value == "":
        return value
    pattern = _QUALIFIED_IDENT_RE if allow_qualified else _BARE_IDENT_RE
    if not pattern.match(value):
        raise SplitError(f"invalid {kind} identifier: {value!r}")
    return value


def validate_target(target):
    """Reject an empty ``--target`` or one containing the queryfile delimiter ``|``.

    Each queryfile line is ``query|target``; a ``|`` in the target (or an empty target)
    would make the loader mis-split the line and route rows to the wrong / no table.
    The pasted-batch path already checks this in ``batch_split.split_query_target``; this
    guards the single ``run_split`` path, which otherwise forwarded ``--target`` raw."""
    t = (target or "").strip()
    if not t or "|" in t:
        raise SplitError(
            f"target must be non-empty and must not contain '|', got {target!r}"
        )
    return t


def _table_regex(owner, table):
    return re.compile(rf"\b{re.escape(owner)}\.{re.escape(table)}\b", re.IGNORECASE)


def detect_alias(query, owner, table):
    """Return the alias used for OWNER.TABLE in a FROM/JOIN clause, or None."""
    pat = re.compile(
        rf"\b{re.escape(owner)}\.{re.escape(table)}\b\s+"
        rf"(?!(?:{'|'.join(_ALIAS_KEYWORDS)})\b)({_IDENT})",
        re.IGNORECASE,
    )
    m = pat.search(query)
    return m.group(1) if m else None


def inject_predicate(query, predicate):
    base = query.rstrip().rstrip(";").rstrip()
    if SPLIT_TOKEN in base:
        return base.replace(SPLIT_TOKEN, predicate)
    if re.search(r"\bWHERE\b", base, re.IGNORECASE):
        return f"{base} AND {predicate}"
    return f"{base} WHERE {predicate}"


# Backwards-compatibility alias
_inject_predicate = inject_predicate


# Matches an existing optimizer-hint block immediately after the leading SELECT:
# groups = (leading "SELECT ", "/*+", inner hint text, "*/").
_HINT_BLOCK_RE = re.compile(r"\A(\s*select\s+)(/\*\+)(.*?)(\*/)", re.IGNORECASE | re.DOTALL)
# Matches the leading SELECT keyword (with surrounding whitespace) for a fresh insert.
_LEADING_SELECT_RE = re.compile(r"(\s*)(select)(\s+)", re.IGNORECASE)


def inject_hint(query, hint):
    """Insert an optimizer hint (e.g. ``PARALLEL(4)``) right after the leading SELECT.

    ``hint`` is the inner hint text WITHOUT the ``/*+ */`` wrapper. Engine-agnostic:
    the dialect decides the hint text (Oracle emits ``PARALLEL(n)``; engines with no
    inline-hint concept return None, and a falsy ``hint`` leaves the query unchanged).

    If a ``/*+ ... */`` block already follows SELECT, the hint is MERGED into it rather
    than adding a second block — unless that block already carries a PARALLEL hint, in
    which case the query is returned untouched (operator's explicit choice wins). Only
    the FIRST SELECT keyword is targeted. Raises SplitError if the query is not a SELECT.
    """
    if not hint:
        return query
    m = _HINT_BLOCK_RE.match(query)
    if m:
        inner = m.group(3)
        if re.search(r"\bPARALLEL\b", inner, re.IGNORECASE):
            return query
        return f"{m.group(1)}/*+ {inner.strip()} {hint} */{query[m.end():]}"
    m2 = _LEADING_SELECT_RE.match(query)
    if not m2:
        raise SplitError("cannot inject hint: query does not start with SELECT")
    return f"{m2.group(1)}{m2.group(2)} /*+ {hint} */ {query[m2.end():]}"


def render_rowid_line(query, alias, lo, hi, target):
    """Build one `query|target` line for a ROWID-range slice."""
    if alias is None and re.search(r"\bJOIN\b", query, re.IGNORECASE):
        raise SplitError(
            "Join query needs the driving table's alias to qualify ROWID; "
            "pass --alias or reference the table as OWNER.TABLE <alias>."
        )
    rowid = f"{alias}.ROWID" if alias else "ROWID"
    predicate = f"{rowid} BETWEEN '{lo}' AND '{hi}'"
    return f"{inject_predicate(query, predicate)}|{target}"


def render_column_range_line(query, col, lo, hi, target, inclusive_hi=False):
    """Build one `query|target` line for a half-open column range.

    Produces `col >= lo AND col < hi` (or `<= hi` when inclusive_hi). `col` is the
    full column reference the caller wants (already alias-qualified if needed, e.g.
    "s.CREATED_DT"). `lo`/`hi` are SQL literals ALREADY FORMATTED by the caller /
    dialect (quoting, TO_DATE(), etc.) — this pure function never quotes or formats
    values, so it stays engine-agnostic. Half-open bounds avoid boundary double-count
    across adjacent slices.
    """
    op_hi = "<=" if inclusive_hi else "<"
    predicate = f"{col} >= {lo} AND {col} {op_hi} {hi}"
    return f"{inject_predicate(query, predicate)}|{target}"


def render_partition_line(query, owner, table, pname, target, sub=False):
    """Build one `query|target` line using a partition-extended table reference."""
    keyword = "SUBPARTITION" if sub else "PARTITION"
    neutralized = query.replace(SPLIT_TOKEN, "1=1") if SPLIT_TOKEN in query else query
    new_query, n = _table_regex(owner, table).subn(
        lambda m: f"{m.group(0)} {keyword} ({pname})", neutralized, count=1
    )
    if n == 0:
        raise SplitError(
            f"Driving table {owner}.{table} not found in query for partition rewrite"
        )
    return f"{new_query}|{target}"


def render_child_table_line(query, owner, table, child, target):
    """Build one `query|target` line that swaps the parent table reference
    OWNER.TABLE for the child relation OWNER.CHILD.

    Used by engines whose partitions are separate child relations (e.g. PostgreSQL
    declarative partitioning): to scan one partition you query its child table
    directly. Any `~SPLIT~` token is neutralized to `1=1`; the table alias (if any)
    is preserved because only the `OWNER.TABLE` token is replaced, not what follows.
    """
    neutralized = query.replace(SPLIT_TOKEN, "1=1") if SPLIT_TOKEN in query else query
    new_query, n = _table_regex(owner, table).subn(
        lambda m: f"{owner}.{child}", neutralized, count=1
    )
    if n == 0:
        raise SplitError(
            f"Driving table {owner}.{table} not found in query for partition rewrite"
        )
    return f"{new_query}|{target}"


def coalesce_ranges(ranges):
    """Merge only forward-adjacent/overlapping (lo, hi) ROWID ranges, preserving the
    DB-provided order. Source boundary methods already return ordered, disjoint
    ranges; a host-side sorted() would impose Python ASCII order (!= Oracle ROWID
    order) and could mis-merge. A range that jumps backwards (lo < the open range's
    lo) is a mis-order: it is appended as-is, never merged across its neighbor."""
    out = []
    for lo, hi in ranges:
        prev = out[-1] if out else None
        if prev and prev[0] <= lo <= prev[1]:
            out[-1] = (prev[0], max(prev[1], hi))
        else:
            out.append((lo, hi))
    return out


def format_lines(lines):
    """Join queryfile lines with a trailing newline."""
    return "\n".join(lines) + "\n"
