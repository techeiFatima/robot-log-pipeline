import duckdb

con = duckdb.connect("cobot.duckdb", read_only=True)

queries = {
    "Rows missing the stop value, and how many still have a sensor reading": """
        SELECT COUNT(*) AS rows_checked,
               COUNT(Current_J1) AS have_current_j1,
               COUNT(Current_J0) AS have_current_j0
        FROM raw_logs
        WHERE Robot_ProtectiveStop IS NULL
    """,
    "Column types": """
        SELECT column_name, column_type
        FROM (DESCRIBE raw_logs)
    """,
}

for title, query in queries.items():
    print(f"\n--- {title} ---")
    print(con.execute(query).df())
