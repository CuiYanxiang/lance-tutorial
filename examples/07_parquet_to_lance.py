"""07_parquet_to_lance.py — 实战：把 Parquet 迁成 Lance（先生成一个样例 parquet 再转）。"""
import lance, pyarrow as pa, pyarrow.parquet as pq
import pyarrow.dataset as pads

# 造一个样例 parquet
tbl = pa.table({"id": range(1000), "score": [i * 0.01 for i in range(1000)]})
pq.write_table(tbl, "./source.parquet")

# 转换
pq_ds = pads.dataset("./source.parquet", format="parquet")
lance.write_dataset(pq_ds.to_table(), "./from_parquet.lance",
                    data_storage_version="2.2", mode="overwrite")

back = lance.dataset("./from_parquet.lance")
assert back.count_rows() == pq_ds.count_rows()
print("converted rows:", back.count_rows())
