"""
Rig report — active-rig wells joined with well_combined, exported to CSV + Parquet.

Runs the validated query (schema confirmed 2026-07-22 against DSD DATA) and
writes a dated CSV (human-readable) and Parquet (compact/typed) to ./exports/.
This is the building block a scheduled sync will later call.

Usage:
    ./venv/bin/python rig_report.py
"""
import datetime
from pathlib import Path

from ed_client import get_connection, fetch_frame

# NOTE on two columns with no source in the schema:
#   RIG_NUM  -> current_rigs has no rig-number column (cast NULL). To populate,
#              change to `cr.name AS RIG_NUM` (rig/well name) or `cr.driller`.
#   DIST_NAME-> no district column (cast NULL). well_combined.rrc_district_number
#              exists for TX wells if you want it wired in later.
QUERY = """
SELECT
    CURRENT_DATE            AS DATE_PROD,
    wc.state_abbr           AS STATE_NAME,
    CAST(NULL AS VARCHAR)   AS DIST_NAME,
    wc.county               AS COUNTY_SL,
    wc.well_name            AS WELLNAM_SL,
    wc.drill_type           AS WELL_ORNT,
    wc.field                AS FIELD_1,
    wc.permit_date          AS DATE_APRV,
    wc.spud_date            AS DATE_SPUD,
    cr.driller              AS DRIL_NAME,
    CAST(NULL AS VARCHAR)   AS RIG_NUM,
    wc.measured_depth       AS P_T_D,
    wc.true_vertical_depth  AS WELL_DEPTH,
    wc.measured_depth       AS MEAS_DEPTH,
    cr.operator_name        AS OPER_NAME
FROM well_combined wc
JOIN current_rigs cr ON wc.well_api = cr.well_api
"""

OUT_DIR = Path(__file__).with_name("exports")


def main():
    OUT_DIR.mkdir(exist_ok=True)
    stamp = datetime.date.today().isoformat()

    conn = get_connection()
    try:
        df = fetch_frame(conn.cursor(), QUERY)
    finally:
        conn.close()

    csv_path = OUT_DIR / f"rig_report_{stamp}.csv"
    pq_path = OUT_DIR / f"rig_report_{stamp}.parquet"
    df.to_csv(csv_path, index=False)
    df.to_parquet(pq_path, index=False)

    print(f"Rows exported : {len(df)}")
    print(f"CSV           : {csv_path}  ({csv_path.stat().st_size:,} bytes)")
    print(f"Parquet       : {pq_path}  ({pq_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
