"""14_schema_evolution.py — 进阶案例：schema 演进（加列不重写全表）。
两种加列方式：
  a) add_columns(SQL 表达式字典)：用现有列推导新列
  b) add_columns(batch_udf)：用 Python UDF 批量回填（如生成 embedding），只写新列文件
"""
import lance, pyarrow as pa

ds = lance.write_dataset(
    pa.table({"id": [1, 2, 3], "price": [100, 200, 300], "qty": [2, 1, 4]}),
    "./evolve.lance", data_storage_version="2.2", mode="overwrite",
)

# a) 用 SQL 表达式加列（引用现有列；注意 add_columns 的 SQL 类型需与列类型匹配，
#    整数列做除法时要先 CAST）
ds.add_columns({"total": "price * qty", "price_x10": "price * 10"})
print(ds.to_table().to_pandas())

# b) 用 batch_udf 回填新列（仅写该列的数据文件）
@lance.batch_udf()
def hash_embedding(batch: pa.RecordBatch) -> pa.RecordBatch:
    import hashlib
    vecs = []
    for v in batch.column("id").to_pylist():
        h = int(hashlib.md5(str(v).encode()).hexdigest(), 16) % 10
        vecs.append([float(h), float(h) / 10.0])
    return pa.RecordBatch.from_arrays(
        [pa.array(vecs, type=pa.list_(pa.float32(), 2))], names=["embedding"])

ds.add_columns(hash_embedding)
print(ds.to_table().to_pandas())
print("schema:", ds.schema.names)
