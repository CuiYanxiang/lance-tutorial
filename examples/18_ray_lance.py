"""18_ray_lance.py — Ray + Lance 集成最佳实践（分布式读/写/数据演进/压实）。
依赖：uv sync --group ray
注意：lance-ray 的 API 以官方为准；本脚本含容错，未装 ray 时打印提示跳过。
"""
import os
import pyarrow as pa

try:
    import ray
    from lance_ray import read_lance, write_lance, add_columns, compact_files
    HAVE_RAY = True
except ImportError:
    HAVE_RAY = False

if not HAVE_RAY:
    print("跳过：未安装 ray/lance-ray。请先执行：uv sync --group ray")
    raise SystemExit(0)

ray.init(ignore_reinit_error=True)

# 1) 分布式写：Ray Dataset -> Lance
data = ray.data.range(1000).map(
    lambda r: {"id": r["id"], "value": r["id"] * 2, "tag": f"t{r['id'] % 5}"})
write_lance(data, "./ray_ds.lance", mode="overwrite", data_storage_version="2.2")

# 2) 分布式读：Lance -> Ray Dataset（列裁剪 + 过滤下推）
ds = read_lance("./ray_ds.lance", columns=["id", "value"], filter="value > 1000")
print("filtered rows:", ds.count())

# 3) 分布式数据演进：Ray UDF 批量回填新列（add_columns，不重写全表）
def add_computed(batch: pa.RecordBatch) -> pa.RecordBatch:
    import pyarrow.compute as pc
    return pa.RecordBatch.from_arrays(
        [pc.multiply(batch.column("value"), 10)], names=["value_x10"])

add_columns(uri="./ray_ds.lance", transform=add_computed, concurrency=4)

# 4) 分布式压实（合并小片段，num_workers 控并行度）
compact_files("./ray_ds.lance", num_workers=4)

# 验证
back = read_lance("./ray_ds.lance")
print("final rows:", back.count(), "schema:", back.schema().names)

ray.shutdown()
