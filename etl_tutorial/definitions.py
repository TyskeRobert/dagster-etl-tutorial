import json
import os

from dagster_duckdb import DuckDBResource

import dagster as dg

defs = dg.Definitions(
    assets=[],
    resources={
        "duckdb": DuckDBResource(database="data/mydb.duckdb")
    }
)
