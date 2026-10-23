# 05 · Ray + Lance 集成最佳实践

> 目标：用 Ray 的分布式计算把 Lance 数据集放大到「读得动、写得起、能演进、能压实」的规模。
> 配套脚本：`examples/18_ray_lance.py`。版本基准：Ray 2.59.0、lance-ray（Lance-Ray 独立子项目）。

---

## 5.1 两条集成路线，先分清

| 路线 | API | 适用 |
|---|---|---|
| **Ray Data 内建** | `ray.data.read_lance(uri, columns=, filter=)`、`ds.write_lance(uri, mode=)` | 轻量：把 Lance 当数据源/目标，做分布式读与写，不依赖额外包 |
| **lance-ray 独立子项目** | `from lance_ray import read_lance, write_lance, add_columns, compact_files, create_scalar_index` | 进阶：分布式**数据演进、索引维护、压实**等需要 Ray 协调能力的管理操作 |

> 结论：**纯读写用 Ray Data 内建；要做特征回填、索引维护、压实这些「数据治理」操作，用 lance-ray。** 两者可在同一工程里混用。

## 5.2 安装

```bash
uv sync --group ray
# 若要预发布版（临时装到当前环境，不进锁文件）
uv pip install lance-ray==0.2.0b1 --extra-index-url https://pypi.fury.io/lance-format/
```

## 5.3 分布式读写（最常用）

```python
import ray
from lance_ray import read_lance, write_lance

ray.init()

# 写：Ray Dataset -> Lance
data = ray.data.range(1000).map(lambda r: {"id": r["id"], "value": r["id"] * 2})
write_lance(data, "./ds.lance", mode="overwrite", data_storage_version="2.2")

# 读：Lance -> Ray Dataset（列裁剪 + 过滤下推，在存储层就减 I/O）
ds = read_lance("./ds.lance", columns=["id", "value"], filter="value > 1000")
filtered = ds.filter(lambda row: row["value"] > 1000)   # 或交给 Ray 继续处理
```

**写模式**：`"create"` / `"append"` / `"overwrite"`。
**关键参数**：`min_rows_per_file`（默认 1024×1024）、`max_rows_per_file`（默认 64×1024×1024）、`concurrency`（最大并发 Ray task）、`data_storage_version`、`ray_remote_args`。

## 5.4 分布式数据演进（add_columns：回填不重写全表）

大表加列/回填（如补 embedding）用 Ray UDF 并行完成，**只写新列的文件**，不重写旧列：

```python
import pyarrow as pa
from lance_ray import add_columns

def add_computed(batch: pa.RecordBatch) -> pa.RecordBatch:
    # 你的计算：如 model.encode(batch["text"])
    return pa.RecordBatch.from_arrays(
        [pa.array([len(t) for t in batch.column("text").to_pylist()])],
        names=["text_len"])

add_columns(uri="./ds.lance", transform=add_computed, concurrency=4)
```

这正好把精通篇 2.4 的 `add_columns(batch_udf)` 放大到分布式：单机 UDF → Ray 多 worker 并行回填。

## 5.5 分布式索引与压实（生产运维的关键）

- **索引**：lance-ray 提供分布式索引构建（`create_scalar_index` 等），适合大数据集建 FTS / BTREE / 向量索引，避免单机建索引成为瓶颈。
- **压实**：高频小 append 产生的碎片，用 Ray 并行压实：

```python
from lance_ray import compact_files
compact_files("./ds.lance", num_workers=4)
```

压实会合并小片段、物理清除软删行；**压实后需重建被影响的 ANN 索引**（和纯 pylance 的纪律一致）。

## 5.6 最佳实践清单（生产级）

1. **读写分离调度**：分布式 ETL/特征工程用 Ray 集群；在线 serving 的随机点读（`take`/`take_blobs`）走单机 pylance，不要为了快而把 serving 也搬到 Ray。
2. **下推优先**：`read_lance` 里尽量用 `columns=` + `filter=` 把裁剪和过滤下推到存储层，Ray 只做剩余计算，I/O 和网络都能省。
3. **写并发有界**：`write_lance` 用 `concurrency=` 限流，避免海量小文件；高频小写后定期 `compact_files`。
4. **压实后再建索引**：索引维护和压实分步走，顺序是「压实 → 重建索引」，否则索引随片段重写失效。
5. **版本治理**：每次写/演进/压实都会 +1 版本，配合 tags 固定发布基线，用 `cleanup_old_versions` 控制磁盘增长（见运维篇）。
6. **Blob V2 引用模式**：多模态大对象可用 `external_blob_mode="reference"` 引用外部 URI（如对象存储），避免把大字节搬进 Ray 内存。
7. **对象存储**：S3/OSS/GS 上运行时配 `storage_options`；writer 若需自动清理要有 delete 权限。

## 5.7 一个典型流水线（Data → Ray → Lance → Serving）

```
上游数据(Parquet/Kafka/API)
   │
   ▼
Ray Dataset ──map/batch inference──▶ 生成 embedding / 特征
   │
   ▼ write_lance
Lance 数据集（Blob + 向量 + 标量同表）
   │
   ├─▶ add_columns 分布式回填新特征
   ├─▶ create_scalar_index / 向量索引
   ├─▶ compact_files 定期压实
   └─▶ Serving：单机 pylance take()/take_blobs() 低延迟点读
```

> 心智模型：**Ray 负责「算」（批量、分布式），Lance 负责「存」（随机访问、版本、索引）**。两者用 Arrow 作为公共内存表达，天然互通。

---

**下一篇任务提示**：以上索引/压实的 lance-ray 精确签名可能随版本演进，生产落地前以 `uv pip show lance-ray` 和官方 `lance.org/integrations/ray/` 为准。
