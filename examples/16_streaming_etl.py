"""16_streaming_etl.py — 进阶案例：流式读（to_batches）+ 生成器流式写。
适合内存放不下的大表 ETL。
"""
import lance, pyarrow as pa
from typing import Iterator

# 造一个大表
big = pa.table({"id": range(200_000), "cat": [f"c{i % 100}" for i in range(200_000)],
                "val": [i * 1.5 for i in range(200_000)]})
ds = lance.write_dataset(big, "./stream.lance", data_storage_version="2.2", mode="overwrite")

# 1) 流式读：分批处理，不一次全载入内存
total = 0
for batch in ds.to_batches(columns=["id", "val"], filter="val > 1000", batch_size=10_000):
    total += batch.num_rows
    # ... 对每个 batch 做计算
print("streamed filtered rows:", total)

# 2) 生成器流式写：从一个 producer（如分片读取）边读边写
def producer() -> Iterator[pa.RecordBatch]:
    for i in range(0, 100, 10):
        yield pa.RecordBatch.from_arrays(
            [pa.array(range(i, i + 10)), pa.array([x * 2.0 for x in range(i, i + 10)])],
            names=["id", "val"])

schema = pa.schema([("id", pa.int64()), ("val", pa.float64())])
streamed = lance.write_dataset(producer(), "./stream_out.lance", schema=schema,
                               mode="overwrite", data_storage_version="2.2")
print("streamed written rows:", streamed.count_rows())
