"""10_duckdb_spark.py — 实战：不搬数据，用 DuckDB / Spark 直接查 Lance。

DuckDB（2026 版已支持扫描 Lance）：
    uv add duckdb
    python -c "import duckdb; print(duckdb.sql(\"SELECT * FROM './from_parquet.lance' WHERE id < 3\"))"

Spark（需 Lance Spark connector，集群环境）：
    df = spark.read.format(\"lance\").load(\"s3://bucket/train.lance\")
"""
print(__doc__)
