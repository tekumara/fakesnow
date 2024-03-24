import sys

import snowflake.connector

names = sys.argv[1:] if len(sys.argv) > 1 else ["world"]

conn = snowflake.connector.connect()

print(
    conn.cursor().execute(f"SELECT 'Hello fake {' '.join(names)}!'").fetchone()  # pyright: ignore[reportOptionalMemberAccess]
)
