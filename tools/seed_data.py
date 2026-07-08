#!/usr/bin/env python3
"""
Seed script: clears PAY feedback tables and repopulates with ~900k rows.
Requires the oracle19c Docker container to be running.
Execution time: ~5–15 minutes depending on host hardware.
"""

import subprocess
import sys
import tempfile
import os
import random

DOCKER_CONTAINER = "oracle19c"
ORACLE_SID = "ORCL"
CONTAINER_TMP = "/tmp/seed_payload.sql"

ENTITY_TYPES = [
    "CASE", "OBJECT", "INCIDENT", "REQUEST", "TASK",
    "CHANGE", "PROBLEM", "RELEASE", "KNOWLEDGE", "SURVEY",
    "FEEDBACK_FORM", "COMPLAINT", "INQUIRY", "ESCALATION", "REVIEW",
]

TENANTS = ["tenant_1", "tenant_2", "tenant_3", "tenant_4", "tenant_5"]
QUESTIONS = ["QUES-1", "QUES-2", "QUES-3", "QUES-4", "QUES-5"]
RESPONSES = ["Strongly Agree", "Agree", "Neutral", "Disagree", "Strongly Disagree"]
ADTNL_RESPONSES = [
    "Follow-up comment A", "Follow-up comment B",
    "N/A", "See attached notes", "No additional comments",
]
MAP_VALUES = ["active", "archived", "pending", "reviewed", "closed"]
KEY_IDS = ["KEY_1", "KEY_2", "KEY_3", "KEY_4", "KEY_5"]


def run_sql_file(local_path: str) -> None:
    """Copy a SQL file into the container and execute it with sqlplus as sysdba."""
    subprocess.run(
        ["docker", "cp", local_path, f"{DOCKER_CONTAINER}:{CONTAINER_TMP}"],
        check=True,
    )
    # docker cp preserves host file ownership; chmod so the oracle user can read it
    subprocess.run(
        ["docker", "exec", "--user", "root", DOCKER_CONTAINER,
         "chmod", "644", CONTAINER_TMP],
        check=True,
    )
    cmd = [
        "docker", "exec", DOCKER_CONTAINER,
        "bash", "-c",
        f"ORACLE_SID={ORACLE_SID}; export ORACLE_SID; "
        f"sqlplus -s '/ as sysdba' @{CONTAINER_TMP}",
    ]
    result = subprocess.run(cmd, capture_output=True, timeout=1800)
    output = result.stdout.decode()
    stderr_output = result.stderr.decode()
    if stderr_output.strip():
        print(f"[STDERR]\n{stderr_output[:500]}", file=sys.stderr)
    if "ORA-" in output or "SP2-" in output:
        print(f"[SQL ERROR]\n{output[:1000]}", file=sys.stderr)
        raise RuntimeError("sqlplus error detected in output")
    if result.returncode != 0:
        print(f"[SQL ERROR] returncode={result.returncode}\n{output[:1000]}", file=sys.stderr)
        raise RuntimeError(f"sqlplus exited with code {result.returncode}")
    print(output.strip())


def build_plsql_block(
    entity_type: str,
    row_count: int,
    entity_id_start: int,
) -> str:
    """
    Returns a self-contained PL/SQL anonymous block that inserts `row_count`
    rows into all 4 source tables for the given entity_type.
    entity_id_start is the first ENTITY_ID value for this block.
    TUIDs are derived deterministically from the counter so child rows
    never need to re-query the parent.
    """
    return f"""
DECLARE
  v_row_count    NUMBER := {row_count};
  v_id_start     NUMBER := {entity_id_start};
  v_etype        VARCHAR2(50) := '{entity_type}';
  v_counter      NUMBER;
  v_sub_tuid     VARCHAR2(255);
  v_resp1_tuid   VARCHAR2(255);
  v_resp2_tuid   VARCHAR2(255);
  v_adl1_tuid    VARCHAR2(255);
  v_adl2_tuid    VARCHAR2(255);
  v_map_tuid     VARCHAR2(255);
  v_tenant       VARCHAR2(50);
  v_submitter    VARCHAR2(50);
  v_recipient    VARCHAR2(50);
  v_template     VARCHAR2(50);
  v_is_anon      VARCHAR2(10);
  v_ques1        VARCHAR2(50);
  v_ques2        VARCHAR2(50);
  v_resp1_text   VARCHAR2(100);
  v_resp2_text   VARCHAR2(100);
  v_adl_text     VARCHAR2(100);
  v_map_val      VARCHAR2(50);
  v_key_id       VARCHAR2(50);
  v_time         NUMBER := 1776887488;
  TENANTS        DBMS_SQL.VARCHAR2_TABLE;
  QUESTIONS      DBMS_SQL.VARCHAR2_TABLE;
  RESPONSES      DBMS_SQL.VARCHAR2_TABLE;
  ADTNL          DBMS_SQL.VARCHAR2_TABLE;
  MAP_VALS       DBMS_SQL.VARCHAR2_TABLE;
  KEY_IDS        DBMS_SQL.VARCHAR2_TABLE;
BEGIN
  TENANTS(1) := 'tenant_1'; TENANTS(2) := 'tenant_2'; TENANTS(3) := 'tenant_3';
  TENANTS(4) := 'tenant_4'; TENANTS(5) := 'tenant_5';
  QUESTIONS(1) := 'QUES-1'; QUESTIONS(2) := 'QUES-2'; QUESTIONS(3) := 'QUES-3';
  QUESTIONS(4) := 'QUES-4'; QUESTIONS(5) := 'QUES-5';
  RESPONSES(1) := 'Strongly Agree'; RESPONSES(2) := 'Agree';
  RESPONSES(3) := 'Neutral'; RESPONSES(4) := 'Disagree';
  RESPONSES(5) := 'Strongly Disagree';
  ADTNL(1) := 'Follow-up comment A'; ADTNL(2) := 'Follow-up comment B';
  ADTNL(3) := 'N/A'; ADTNL(4) := 'See attached notes';
  ADTNL(5) := 'No additional comments';
  MAP_VALS(1) := 'active'; MAP_VALS(2) := 'archived'; MAP_VALS(3) := 'pending';
  MAP_VALS(4) := 'reviewed'; MAP_VALS(5) := 'closed';
  KEY_IDS(1) := 'KEY_1'; KEY_IDS(2) := 'KEY_2'; KEY_IDS(3) := 'KEY_3';
  KEY_IDS(4) := 'KEY_4'; KEY_IDS(5) := 'KEY_5';

  FOR i IN 1..v_row_count LOOP
    v_counter    := v_id_start + i - 1;
    v_sub_tuid   := 'SUB-' || TO_CHAR(v_counter);
    v_resp1_tuid := 'RESP-' || TO_CHAR(v_counter) || '-1';
    v_resp2_tuid := 'RESP-' || TO_CHAR(v_counter) || '-2';
    v_adl1_tuid  := 'ADL-' || TO_CHAR(v_counter) || '-1';
    v_adl2_tuid  := 'ADL-' || TO_CHAR(v_counter) || '-2';
    v_map_tuid   := 'MAP-' || TO_CHAR(v_counter);
    v_tenant     := TENANTS(MOD(v_counter, 5) + 1);
    v_submitter  := 'user_' || TO_CHAR(MOD(v_counter, 20) + 1);
    v_recipient  := 'rec_' || TO_CHAR(MOD(v_counter, 10) + 1);
    v_template   := 'TMPL-' || TO_CHAR(MOD(v_counter, 5) + 1);
    v_is_anon    := CASE WHEN MOD(v_counter, 3) = 0 THEN 'Y' ELSE 'N' END;
    v_ques1      := QUESTIONS(MOD(v_counter, 5) + 1);
    v_ques2      := QUESTIONS(MOD(v_counter + 2, 5) + 1);
    v_resp1_text := RESPONSES(MOD(v_counter, 5) + 1);
    v_resp2_text := RESPONSES(MOD(v_counter + 1, 5) + 1);
    v_adl_text   := ADTNL(MOD(v_counter, 5) + 1);
    v_map_val    := MAP_VALS(MOD(v_counter, 5) + 1);
    v_key_id     := KEY_IDS(MOD(v_counter, 5) + 1);

    INSERT INTO PAY.CM_FB_SUBMISSION
      (CM_FB_SUBMISSION_TUID, ENTITY_ID, CM_FB_TEMPLATE_TUID,
       SUBMITTED_BY, RECIPIENT_ID, RECIPIENT_TYPE,
       ENTITY_TYPE, IS_ANONYMOUS, TIME_CREATED, TENANT_NAME)
    VALUES
      (v_sub_tuid, TO_CHAR(v_counter), v_template,
       v_submitter, v_recipient, 'EMPLOYEE',
       v_etype, v_is_anon, v_time, v_tenant);

    INSERT INTO PAY.CM_FB_SUBMSN_RESP
      (CM_FB_SUBMSN_RESP_TUID, CM_FB_TEMPLATE_QUES_TUID,
       CM_FB_SUBMISSION_TUID, FB_RESPONSE, IS_ACTIVE,
       TIME_CREATED, TIME_UPDATED, TENANT_NAME)
    VALUES
      (v_resp1_tuid, v_ques1, v_sub_tuid, v_resp1_text, 'Y',
       v_time, v_time, v_tenant);

    INSERT INTO PAY.CM_FB_SUBMSN_RESP
      (CM_FB_SUBMSN_RESP_TUID, CM_FB_TEMPLATE_QUES_TUID,
       CM_FB_SUBMISSION_TUID, FB_RESPONSE, IS_ACTIVE,
       TIME_CREATED, TIME_UPDATED, TENANT_NAME)
    VALUES
      (v_resp2_tuid, v_ques2, v_sub_tuid, v_resp2_text, 'Y',
       v_time, v_time, v_tenant);

    INSERT INTO PAY.CM_FB_SUB_ADTNL_RESP
      (CM_FB_SUB_ADTNL_RESP_TUID, CM_FB_SUBMSN_RESP_TUID,
       FB_ADTNL_RESPONSE, IS_ACTIVE,
       TIME_CREATED, TIME_UPDATED, TENANT_NAME)
    VALUES
      (v_adl1_tuid, v_resp1_tuid, v_adl_text,
       CASE WHEN MOD(v_counter, 2) = 0 THEN 'Y' ELSE 'N' END,
       v_time, v_time, v_tenant);

    INSERT INTO PAY.CM_FB_SUB_ADTNL_RESP
      (CM_FB_SUB_ADTNL_RESP_TUID, CM_FB_SUBMSN_RESP_TUID,
       FB_ADTNL_RESPONSE, IS_ACTIVE,
       TIME_CREATED, TIME_UPDATED, TENANT_NAME)
    VALUES
      (v_adl2_tuid, v_resp2_tuid, ADTNL(MOD(v_counter + 1, 5) + 1),
       CASE WHEN MOD(v_counter, 2) = 1 THEN 'Y' ELSE 'N' END,
       v_time, v_time, v_tenant);

    INSERT INTO PAY.CM_FB_SUBMISSION_MAP
      (CM_FB_SUBMSN_MAP_TUID, CM_FB_SUBMISSION_TUID,
       CM_FB_TMPLT_SUB_KEY_ID, MAP_VALUE,
       TIME_CREATED, TENANT_NAME)
    VALUES
      (v_map_tuid, v_sub_tuid, v_key_id, v_map_val,
       v_time, v_tenant);

    IF MOD(i, 10000) = 0 THEN
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('  [{entity_type}] ' || TO_CHAR(i) || ' / ' || TO_CHAR(v_row_count) || ' committed');
    END IF;
  END LOOP;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('[DONE] {entity_type}: ' || TO_CHAR(v_row_count) || ' submissions inserted (ENTITY_ID ' || TO_CHAR(v_id_start) || '..' || TO_CHAR(v_id_start + v_row_count - 1) || ')');
END;
/
"""


def seed_all_types(min_rows: int = 50_000, max_rows: int = 70_000) -> dict:
    """
    Generates and executes one PL/SQL block per entity type.
    Returns a dict mapping entity_type -> row_count for use in queryfile generation.
    """
    random.seed(42)  # reproducible counts across runs
    type_counts = {
        et: random.randint(min_rows, max_rows) for et in ENTITY_TYPES
    }

    entity_id_cursor = 1
    for entity_type, row_count in type_counts.items():
        print(f"\n[{entity_type}] Seeding {row_count:,} rows (ENTITY_ID {entity_id_cursor:,}–{entity_id_cursor + row_count - 1:,})...")
        plsql = build_plsql_block(entity_type, row_count, entity_id_cursor)
        header = "SET ECHO OFF SERVEROUTPUT ON SIZE UNLIMITED\n"
        footer = "\nEXIT;\n"
        full_sql = header + plsql + footer

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(full_sql)
            tmp = f.name
        try:
            run_sql_file(tmp)
        finally:
            os.unlink(tmp)

        entity_id_cursor += row_count

    total = sum(type_counts.values())
    print(f"\nSeeding complete. Total submissions: {total:,}")
    return type_counts


def truncate_tables() -> None:
    print("Truncating tables (child → parent order)...")
    # TRUNCATE is DDL: auto-commits instantly, no undo log, cannot leave partial state.
    sql = (
        "SET ECHO OFF FEEDBACK OFF\n"
        "TRUNCATE TABLE PAY.CM_FB_SUB_ADTNL_RESP;\n"
        "TRUNCATE TABLE PAY.CM_FB_SUBMSN_RESP;\n"
        "TRUNCATE TABLE PAY.CM_FB_SUBMISSION_MAP;\n"
        "TRUNCATE TABLE PAY.CM_FB_SUBMISSION;\n"
        "TRUNCATE TABLE PAY.FEEDBACK_SUBMISSION;\n"
        "SELECT 'Truncate complete' FROM DUAL;\n"
        "EXIT;\n"
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql)
        tmp = f.name
    try:
        run_sql_file(tmp)
    finally:
        os.unlink(tmp)
    print("  All tables cleared.")


if __name__ == "__main__":
    truncate_tables()
    type_counts = seed_all_types()
    print("\nAll tables seeded. Run generate_queryfile.py to update queryfile.txt.")
