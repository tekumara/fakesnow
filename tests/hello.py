import sys

import snowflake.connector

name = sys.argv[1] if len(sys.argv) > 1 else "world"

conn = snowflake.connector.connect()

print(conn.cursor().execute(f"SELECT 'Hello fake {name}!'").fetchone())  # pyright: ignore[reportOptionalMemberAccess]
