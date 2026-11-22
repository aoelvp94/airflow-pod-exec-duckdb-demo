"""Generate a synthetic dbt project with N models, to see how each execution mode grows with project size.

    python scale/gen.py 200 /app/scale/scale_200

Shape: layers of WIDTH models. Layer 0 reads `range()`; every later model joins two models of the layer
before it, so the DAG is N/WIDTH deep and WIDTH wide, like a staging -> intermediate -> marts project.
Every model is a small table (10k rows), so the time measured is dbt and orchestration, not DuckDB work.
Uses the demo's profile (`duckdb_exec_demo`, /app/dbt/profiles.yml) and writes to schema main_scale_<N>.
"""

from __future__ import annotations

import sys
from pathlib import Path

WIDTH = 10
ROWS = 10_000


def generate(n: int, out: Path) -> None:
    models = out / "models"
    models.mkdir(parents=True, exist_ok=True)
    name = f"scale_{n}"
    (out / "dbt_project.yml").write_text(
        f'name: {name}\nversion: "1.0.0"\nprofile: duckdb_exec_demo\nmodel-paths: ["models"]\n'
        f"models:\n  {name}:\n    +materialized: table\n    +schema: {name}\n"
    )
    for i in range(n):
        model = f"s{n}_m{i:04d}"
        pos = i % WIDTH
        if i < WIDTH:
            sql = f"select i as id, (i * {i + 1}) % 97 as k from range({ROWS}) t(i)"
        else:
            start = (i // WIDTH - 1) * WIDTH  # first model of the previous layer
            a, b = f"s{n}_m{start + pos:04d}", f"s{n}_m{start + (pos + 1) % WIDTH:04d}"
            sql = (
                f"select a.id, (a.k + b.k) % 97 as k\n"
                f"from {{{{ ref('{a}') }}}} a\njoin {{{{ ref('{b}') }}}} b using (id)"
            )
        (models / f"{model}.sql").write_text(sql + "\n")


if __name__ == "__main__":
    generate(int(sys.argv[1]), Path(sys.argv[2]))
