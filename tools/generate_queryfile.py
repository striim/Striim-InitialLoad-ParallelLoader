"""
generate_queryfile.py — Queryfile generator for Striim InitialLoad ParallelLoader.

Generates pipe-delimited query|target_table lines for entity-type-based chunks
and optional simple table loads.

Usage:
  python generate_queryfile.py                     # interactive mode
  python generate_queryfile.py --list              # print entity types and exit
  python generate_queryfile.py --entity-types CASE,INCIDENT --no-include-simple
"""

import argparse
import sys

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ENTITY_TYPES = [
    "CASE", "OBJECT", "INCIDENT", "REQUEST", "TASK",
    "CHANGE", "PROBLEM", "RELEASE", "KNOWLEDGE", "SURVEY",
    "FEEDBACK_FORM", "COMPLAINT", "INQUIRY", "ESCALATION", "REVIEW",
]

DEFAULT_TARGET = "PAY.FEEDBACK_SUBMISSION"
OUTPUT_FILE = "queryfile.txt"

SIMPLE_SUBMISSION_TEMPLATE = (
    "SELECT * FROM PAY.CM_FB_SUBMISSION "
    "WHERE ENTITY_TYPE = '{entity_type}'"
)
TENANT_NAMES = ["tenant_1", "tenant_2", "tenant_3", "tenant_4", "tenant_5"]

SIMPLE_MAP_TEMPLATE = (
    "SELECT * FROM PAY.CM_FB_SUBMISSION_MAP "
    "WHERE TENANT_NAME = '{tenant_name}'"
)
SIMPLE_SUBMISSION_TARGET = "PAY.CM_FB_SUBMISSION_TGT"
SIMPLE_MAP_TARGET = "PAY.CM_FB_SUBMISSION_MAP_TGT"

QUERY_TEMPLATE = (
    "SELECT s.ENTITY_ID, s.ENTITY_TYPE, s.IS_ANONYMOUS, "
    "(SELECT JSON_OBJECT('questions' VALUE JSON_ARRAYAGG("
    "JSON_OBJECT('cm_fb_submsn_resp_tuid' VALUE r.CM_FB_SUBMSN_RESP_TUID, "
    "'cm_fb_template_ques_tuid' VALUE r.CM_FB_TEMPLATE_QUES_TUID, "
    "'fb_response' VALUE r.FB_RESPONSE, "
    "'answers' VALUE (SELECT JSON_ARRAYAGG("
    "JSON_OBJECT('cm_fb_sub_adtnl_resp_tuid' VALUE ar.CM_FB_SUB_ADTNL_RESP_TUID, "
    "'fb_adtnl_response' VALUE ar.FB_ADTNL_RESPONSE)) "
    "FROM PAY.CM_FB_SUB_ADTNL_RESP ar "
    "WHERE ar.CM_FB_SUBMSN_RESP_TUID = r.CM_FB_SUBMSN_RESP_TUID)))) "
    "FROM PAY.CM_FB_SUBMSN_RESP r "
    "WHERE r.CM_FB_SUBMISSION_TUID = s.CM_FB_SUBMISSION_TUID) AS FEEDBACK_DATA, "
    "(SELECT JSON_ARRAYAGG("
    "JSON_OBJECT('cm_fb_submission_map_tuid' VALUE m.CM_FB_SUBMSN_MAP_TUID, "
    "'cm_fb_tmplt_sub_key_id' VALUE m.CM_FB_TMPLT_SUB_KEY_ID, "
    "'map_value' VALUE m.MAP_VALUE, "
    "'tenant_name' VALUE m.TENANT_NAME)) "
    "FROM PAY.CM_FB_SUBMISSION_MAP m "
    "WHERE m.CM_FB_SUBMISSION_TUID = s.CM_FB_SUBMISSION_TUID) AS FEEDBACK_MAPPINGS "
    "FROM PAY.CM_FB_SUBMISSION s "
    "WHERE s.ENTITY_TYPE = '{entity_type}'"
)


# ---------------------------------------------------------------------------
# Core generation logic
# ---------------------------------------------------------------------------

def generate(entity_types, target_table, include_simple, output_file):
    """Build queryfile lines and write to output_file."""
    lines = []

    for et in entity_types:
        query = QUERY_TEMPLATE.format(entity_type=et)
        lines.append(f"{query}|{target_table}")

    if include_simple:
        for et in entity_types:
            lines.append(SIMPLE_SUBMISSION_TEMPLATE.format(entity_type=et) + f"|{SIMPLE_SUBMISSION_TARGET}")
        for tn in TENANT_NAMES:
            lines.append(SIMPLE_MAP_TEMPLATE.format(tenant_name=tn) + f"|{SIMPLE_MAP_TARGET}")

    with open(output_file, "w") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"\nWrote {len(lines)} lines to {output_file}")
    print(f"  Complex queries    : {len(entity_types)}")
    if include_simple:
        print(f"  Simple submission  : {len(entity_types)}")
        print(f"  Simple map         : {len(TENANT_NAMES)}")
        print(f"  Simple table loads : {len(entity_types) + len(TENANT_NAMES)} total")
    print(f"  Target table       : {target_table}")


# ---------------------------------------------------------------------------
# Interactive mode
# ---------------------------------------------------------------------------

def interactive_mode():
    """Prompt the user for generation parameters and return them."""
    print("=== Queryfile Generator (interactive mode) ===\n")

    # --- Entity types ---
    print("Available entity types:")
    for i, et in enumerate(ENTITY_TYPES, 1):
        print(f"  {i:2d}. {et}")
    print()
    prompt = (
        'Which entity types to include?\n'
        '  Enter "all", comma-separated numbers (e.g. 1,3,5),\n'
        '  or comma-separated type names (e.g. CASE,INCIDENT)\n'
        '  [default: all]: '
    )
    raw = input(prompt).strip()

    if not raw or raw.lower() == "all":
        selected_types = list(ENTITY_TYPES)
    else:
        parts = [p.strip() for p in raw.split(",")]
        selected_types = []
        errors = []
        for p in parts:
            if p.isdigit():
                idx = int(p) - 1
                if 0 <= idx < len(ENTITY_TYPES):
                    selected_types.append(ENTITY_TYPES[idx])
                else:
                    errors.append(f"  Number {p} out of range (1-{len(ENTITY_TYPES)})")
            else:
                upper = p.upper()
                if upper in ENTITY_TYPES:
                    selected_types.append(upper)
                else:
                    errors.append(f"  Unknown entity type: {p!r}")
        if errors:
            print("Errors:")
            for e in errors:
                print(e)
            sys.exit(1)
        if not selected_types:
            print("No entity types selected. Exiting.")
            sys.exit(1)

    print(f"\nSelected {len(selected_types)} entity type(s): {', '.join(selected_types)}")

    # --- Simple table loads ---
    raw_simple = input("\nInclude simple table load lines? [Y/n]: ").strip().lower()
    include_simple = raw_simple not in ("n", "no")

    # --- Output file ---
    raw_out = input(f"\nOutput file path? [default: {OUTPUT_FILE}]: ").strip()
    output_file = raw_out if raw_out else OUTPUT_FILE

    # --- Target table ---
    raw_target = input(f"Target table for complex queries? [default: {DEFAULT_TARGET}]: ").strip()
    target_table = raw_target if raw_target else DEFAULT_TARGET

    return selected_types, target_table, include_simple, output_file


# ---------------------------------------------------------------------------
# CLI / entry point
# ---------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        description="Generate a pipe-delimited queryfile for Striim InitialLoad ParallelLoader.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--entity-types",
        metavar="TYPE,...",
        help="Comma-separated entity types to include (default: all 15)",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_FILE,
        metavar="FILE",
        help=f"Output file path (default: {OUTPUT_FILE})",
    )
    parser.add_argument(
        "--target-table",
        default=DEFAULT_TARGET,
        metavar="TABLE",
        help=f"Override target table for complex queries (default: {DEFAULT_TARGET})",
    )
    parser.add_argument(
        "--include-simple",
        dest="include_simple",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include the 2 simple table load lines (default: True; use --no-include-simple to disable)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print available entity types and exit",
    )
    return parser


def main():
    # Detect interactive mode: no CLI arguments supplied
    if len(sys.argv) == 1:
        entity_types, target_table, include_simple, output_file = interactive_mode()
        generate(entity_types, target_table, include_simple, output_file)
        return

    parser = build_parser()
    args = parser.parse_args()

    if args.list:
        print("Available entity types:")
        for i, et in enumerate(ENTITY_TYPES, 1):
            print(f"  {i:2d}. {et}")
        sys.exit(0)

    # Resolve entity types from --entity-types flag
    if args.entity_types:
        parts = [p.strip().upper() for p in args.entity_types.split(",") if p.strip()]
        invalid = [p for p in parts if p not in ENTITY_TYPES]
        if invalid:
            parser.error(f"Unknown entity type(s): {', '.join(invalid)}")
        entity_types = parts
    else:
        entity_types = list(ENTITY_TYPES)

    generate(entity_types, args.target_table, args.include_simple, args.output)


if __name__ == "__main__":
    main()
