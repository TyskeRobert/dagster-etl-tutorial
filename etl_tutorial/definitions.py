import json
import os

from dagster_duckdb import DuckDBResource

import dagster as dg

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion"
)
def products(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE products AS (
                SELECT * FROM read_csv_auto('data/products.csv')
            )
            """
        )

        preview_query = "SELECT * FROM products LIMIT 10"
        preview_df = conn.execute(preview_query).fetch_df()
        row_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False))
            }
        )
    
@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion"
)
def sales_reps(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE sales_reps AS (
                SELECT * FROM read_csv_auto('data/sales_reps.csv')
            )
            """
        )

        preview_query = "SELECT * FROM sales_reps LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM sales_reps").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False))
            }
        )
    
@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion"
)
def sales_data(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            DROP TABLE IF EXISTS sales_data;
            CREATE TABLE sales_data AS SELECT * FROM read_csv_auto('data/sales_data.csv')
            """
        )

        preview_query = "SELECT * FROM sales_data LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM sales_data").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False))
            }
        )

defs = dg.Definitions(
    assets=[
        products,
        sales_reps,
        sales_data
    ],
    resources={
        "duckdb": DuckDBResource(database="data/mydb.duckdb")
    }
)
