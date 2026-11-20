"""Run one read-only SQL query against the warehouse and print the rows as JSON (last line = XCom)."""
import json
import os
import sys

import duckdb

sql = sys.argv[1]
with duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True) as con:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
print(json.dumps(rows, default=str))
