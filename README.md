# Lance 从入门到运维・完整教程

> 面向数据 / AI 工程师的 Lance 湖仓格式实战教程。全部代码基于 **Lance SDK 1.0.x（Rust 内核 + Python `pylance` 绑定）**、**文件格式 `data_storage_version="2.2"`（生产稳定）** 编写，示例可直接运行。



***

## 0. 这套教程讲什么、不讲什么

**讲的是 Lance 格式本身**（开源、Apache-2.0、`uv add pylance`、`import lance`）：

它是一个为多模态 AI 设计的**湖仓（lakehouse）格式**，把三层合三为一：



| 层                 | 类比                     | Lance 中的体现                         |
| ----------------- | ---------------------- | ---------------------------------- |
| 文件格式              | Parquet                | 一个个 `.lance` 列存数据文件                |
| 表格式（table format） | Iceberg                | MVCC / ACID / 版本 /schema 演进 / 二级索引 |
| Catalog 规格        | Hive Metastore / Unity | 数据集自描述、多 base 布局                   |

**一句话定位**：Parquet 擅长「扫列」，但在随机 blob 读取、schema 演进、压缩上暴露出行组（row-group）架构的天花板；Lance 用自有的行寻址 + 文件布局，把**随机访问做到比 Parquet/Iceberg 快约 100×**，同时不牺牲扫描性能，并把向量检索、全文检索、索引内建进格式规范。

**不讲**：`lancedb`（LanceDB 是构建在 Lance 之上的嵌入式向量数据库产品，API 是 `lancedb.connect()`，与本教程的 `import lance` 是两层东西）。本教程只在需要区分二者时点一句。

## 1. 版本基线（写于 2026-10）



| 组件           | 版本                           | 说明                                                  |
| ------------ | ---------------------------- | --------------------------------------------------- |
| Lance 内核 SDK | **1.0.x**（2025-12 GA）        | Rust 内核 + Python/Java 绑定，社区化发版                      |
| Python 包     | `pylance`**&#x20;1.0.x**     | `import lance`；底层即上述 SDK                            |
| 文件格式         | `data_storage_version="2.2"` | 生产推荐：Blob V2、字典值压缩（文本数据较 Parquet 省约 50% 存储）、随机读大幅提速 |
| `2.3`        | 实验态                          | 官方明确：不稳定，某一版写出的文件后续修订可能读不回，**只用于实验**                |
| LanceDB      | 0.40.x                       | 上层向量数据库，本教程不展开                                      |
| Ray 集成      | Ray 2.59.0 + `lance-ray`      | 分布式读/写/数据演进/压实，见 [05-ray集成.md](docs/05-ray集成.md)              |
| Spark 集成    | Spark 3.5 + `org.lance:lance-spark-bundle-3.5_2.12:0.4.0` | DSv2 读写 + SQL/MERGE，见 [06-spark集成.md](docs/06-spark集成.md)   |

> 选型结论：
>
> **新建数据集一律&#x20;**
>
> `data_storage_version="2.2"`
>
> 。
>
> `2.0/2.1`
>
>  是历史默认；不要在生产写 
>
> `2.3`
>
> 。

## 2. 环境准备（uv + Python 3.12）



```
# 假定已装 uv
uv sync                            # 创建 .venv（Python 3.12）并按 pyproject.toml + uv.lock 安装依赖
source .venv/bin/activate          # Windows: .venv\Scripts\activate
# Ray 集成示例（18_ray_lance.py）额外执行：uv sync --group ray
```

验证：



```
uv run python -c "import lance; print(lance.__version__)"
```

支持的存储后端：本地文件系统、AWS `s3://`、GCP `gs://`、HuggingFace Hub `hf://`（S3 类后端需要额外凭据配置，见精通篇「对象存储」）。

## 3. 教程地图



| 章节 | 文档                             | 配套脚本（`examples/`）                                                                                   | 读完你能做什么                                                                    |
| -- | ------------------------------ | --------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| 入门 | [docs/01-入门.md](docs/01-入门.md) | `01_quickstart.py` `02_crud.py`                                                                     | 建表、读子集、增删改、看懂 `.lance` 目录                                                  |
| 精通 | [docs/02-精通.md](docs/02-精通.md) | `03_merge_upsert.py` `04_versioning.py` `05_vector_fts.py` `06_blob.py`                             | Merge/Upsert、schema 演进、IVF-PQ/FTS/ 混合检索、tags/branches/time travel、Blob 多模态 |
| 实战 | [docs/03-实战.md](docs/03-实战.md) | `07_parquet_to_lance.py` `08_pytorch_dataloader.py` `09_rag.py` `10_duckdb_spark.py` `11_hf_hub.py` | Parquet 迁移、多模态训练集、RAG、与 DuckDB/Spark 互通、HF Hub 分发                          |
| 运维 | [docs/04-运维.md](docs/04-运维.md) | `12_maintenance.py` `13_concurrency.py`                                                             | 压实、版本清理、自动清理、并发 / MVCC、对象存储运维、回滚与排障                                        |
| 进阶案例 | [docs/01-入门.md](docs/01-入门.md)~[docs/04-运维.md](docs/04-运维.md) | `14_schema_evolution.py` `15_nested_and_filter.py` `16_streaming_etl.py` `17_take_sample_serving.py` | schema 演进回填、嵌套类型 + 复杂过滤、流式 ETL、随机访问 / Blob V2 serving                    |
| Ray 集成 | [docs/05-ray集成.md](docs/05-ray集成.md) | `18_ray_lance.py`                                                                                   | 分布式读 / 写 / 数据演进 / 压实，Ray 2.59 + lance-ray 最佳实践                             |
| Spark 集成 | [docs/06-spark集成.md](docs/06-spark集成.md) | `19_spark_lance.py`                                                                                 | DSv2 读写、INSERT / MERGE upsert、SQL 分析，org.lance 0.4.0 connector 最佳实践          |

建议顺序：入门 → 精通（每节跑对应脚本）→ 进阶案例（14–17）→ 挑一个实战场景落地 → Ray 集成（18）→ Spark 集成（19）→ 上线前按运维篇 checklist 过一遍。

## 4. 5 分钟最小示例



```
import lance, pyarrow as pa

# 写
tbl = pa.table({"id": [1, 2, 3], "text": ["hello", "lance", "world"]})
ds = lance.write_dataset(tbl, "./demo.lance", data_storage_version="2.2")

# 读：投影 + 过滤下推
print(ds.to_table(columns=["text"], filter="id >= 2").to_pandas())

# 追加 + 删 + 改（每次都产生新版本）
ds.insert(pa.table({"id": [4], "text": ["again"]}))
ds.delete("id = 1")
ds.update({"text": "'LANCE'"}, where="text = 'lance'")

# 随机取行（Lance 的杀手锏）
print(ds.take([0, 2], columns=["text"]).to_pandas())
```

跑通它，你就已经站在入门篇的终点线上了。