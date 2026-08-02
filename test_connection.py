"""Smoke test: connect to Energy Domain and pull a few rows."""
from ed_client import get_connection, fetch_frame

def main():
    conn = get_connection()
    try:
        print("Connected. is_closed =", conn.is_closed)
        cur = conn.cursor()
        # DataFrame path (with type conversion)
        df = fetch_frame(cur, "SELECT * FROM well_combined LIMIT 5")
        print("\nColumns / dtypes:")
        print(df.dtypes)
        print("\nFirst rows:")
        print(df.head())
    finally:
        conn.close()
        print("\nConnection closed.")

if __name__ == "__main__":
    main()
