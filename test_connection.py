"""Smoke test: connect to Energy Domain and pull a few rows.

Uses the RAW CURSOR (cur.execute -> cur.fetchall -> cur.rows/cur.headers), the
same path the production reports use. It deliberately avoids datastream_direct's
fetch_frame(): that helper predates pandas 3.0's string dtype and raises
"Invalid value ... for dtype 'str'" on boxes running pandas 3.x (e.g. prod2).
The raw cursor sidesteps that, so this test passes on every box.
"""
from ed_client import get_connection


def main():
    conn = get_connection()
    try:
        print("Connected. is_closed =", conn.is_closed)
        cur = conn.cursor()
        cur.execute(
            "SELECT state_abbr, county, well_name, measured_depth "
            "FROM well_combined LIMIT 5"
        )
        cur.fetchall()
        headers = list(cur.headers)
        rows = list(cur.rows)
        print(f"\nColumns ({len(headers)}): {headers}")
        print(f"\nFirst {len(rows)} rows:")
        for r in rows:
            print("  ", r)
    finally:
        conn.close()
        print("\nConnection closed. OK.")


if __name__ == "__main__":
    main()
