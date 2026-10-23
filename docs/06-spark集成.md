# 06 · Spark + Lance 集成最佳实践

> 目标：让 Spark 的 SQL/JVM 生态直接读写 Lance 湖仓，做分布式 ETL、SQL 分析、ML 训练数据准备，并在同数据上做向量/全文/hybrid 搜索。
> 配套脚本：`examples/19_spark_lance.py`。版本基准：Apache Spark Connector for Lance **0.4.0**、Spark 3.5。

---

## 6.1 它解决什么

前几篇里，Lance 的 Python 生态（pylance/Ray）是主力；但企业里大量数据能力沉淀在 **Spark SQL / JVM 生态**（现有 ETL、数仓任务）。Spark Lance Connector 用 **DatasourceV2 (DSv2)** 把 Lance 变成一个 Spark 数据源，让：

- `spark.read.format("lance")` 并行扫描 Lance 数据集（列裁剪 + 过滤下推）
- `df.write.format("lance")` / `INSERT` / `MERGE` 写回 Lance（复用 Spark 的分布式写入）
- **同一份数据**上跑 SQL 分析 + 向量搜索 + 全文搜索 + hybrid 检索

一句话：**把 Lance 接入你已有的 Spark 资产，而不是让 Spark 团队学一套新工具。**

## 6.2 安装与兼容性（官方核实）

**Maven Central 坐标（`org.lance` 命名空间）**，选对 Scala/Spark 版本：

| 构件 | 名称 | 示例 |
|---|---|---|
| Base Jar（内部用） | `lance-spark-base_<scala>` | `lance-spark-base_2.12` |
| Lean Jar（自管依赖） | `lance-spark-<spark>_<scala>` | `lance-spark-3.5_2.12` |
| **Bundled Jar（推荐）** | `lance-spark-bundle-<spark>_<scala>` | `lance-spark-bundle-3.5_2.12` |

**Spark 3.5 / Scala 2.12 的精确坐标**（当前版本 0.4.0）：
```xml
<dependency>
  <groupId>org.lance</groupId>
  <artifactId>lance-spark-bundle-3.5_2.12</artifactId>
  <version>0.4.0</version>
  <scope>provided</scope>
</dependency>
```
Spark 集群动态加载（无需改 jar）：
```bash
spark-submit \
  --packages org.lance:lance-spark-bundle-3.5_2.12:0.4.0 \
  your_job.py
```

**兼容矩阵（官方）：**
- **Java**：11（最低）/ 17（推荐 LTS）；**不要用 Java 8**
- **Scala**：2.12 / 2.13 支持；Scala 3 不支持
- **Spark**：3.4 / 3.5 / 4.0 / 4.1 / 4.2 支持（Spark 4.x 仅 Scala 2.13）；Spark 2.x、3.1~3.3 不支持/未测
- **OS**：Linux x86_64/ARM64、macOS Intel/Apple Silicon（ARM64 需 Rosetta）；Windows 在建

## 6.3 读：`spark.read.format("lance")`

```scala
// Scala / Spark SQL
val df = spark.read
  .format("lance")
  .option("batch_size", "4096")        // 大扫描提升吞吐
  .load("/path/to/dataset.lance")      // 本地或 s3:// / gs://

// 列裁剪 + 过滤下推（下推到存储层，减少 I/O）
val filtered = spark.read.format("lance")
  .load("s3://bucket/data.lance")
  .select("id", "embedding")
  .filter("score > 0.8")
```

```python
# PySpark（配合本教程 examples/19_spark_lance.py）
df = spark.read.format("lance").load("./dataset.lance")
df.select("id", "embedding").filter("score > 0.8").show()
```

DSv2 提供**并行扫描**：数据分片（fragment）分布到 Spark executor 并行读。

## 6.4 写：`df.write.format("lance")` 与 SQL DML

```scala
df.write
  .format("lance")
  .mode("append")       // append / overwrite / ignore
  .save("/path/to/dataset.lance")
```

**SQL DDL/DML**（把 Lance 表注册为 SQL 表后）：

```sql
-- 建表
CREATE TABLE my_table USING lance LOCATION 's3://bucket/data.lance'
-- 或
CREATE TABLE my_table (id BIGINT, embedding ARRAY<FLOAT>, score DOUBLE)
USING lance LOCATION 's3://bucket/data.lance'
TBLPROPERTIES ('file_format_version'='2.2');

-- 追加
INSERT INTO my_table SELECT ... FROM source_df;

-- Upsert（合并：命中更新 / 未命中插入）—— Spark 3.4+ 的 MERGE
MERGE INTO my_table t USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.score = s.score
WHEN NOT MATCHED THEN INSERT *;
```

> 写 Blob V2 列：设 `file_format_version` 为 `2.2`+，并在表属性里标 `lance.encoding=blob`（见官方 Config）。float16 向量列用 `ARRAY<FLOAT>` + 表属性 `<column>.arrow.float16='true'`。

## 6.5 SQL 搜索表函数：向量 / 全文 / 混合（经 Lance namespace）

Connector 还暴露 SQL 搜索函数（需 Lance namespace 集成），可在 Spark SQL 里做检索：

```sql
-- 向量检索（假想签名，落地以官方为准）
SELECT * FROM vector_search('my_table', 'embedding', query_vec);
-- 全文检索 / hybrid 检索同理
```

> ⚠️ 这类搜索表函数依赖 Lance namespace 与具体 Spark 版本的能力，**上线前以官方 `lance.org/integrations/spark/` 当前文档为准**。

## 6.6 与 Ray 集成的分工（选型要点）

| 维度 | Ray + Lance | Spark + Lance |
|---|---|---|
| 生态 | Python 原生（pandas/Arrow/ML） | SQL + JVM（Scala/Java、现有数仓任务） |
| 强项 | batch inference、特征工程、Python ML | 大规模 SQL ETL、Join、存量 Spark 任务 |
| 搜索 | `read_lance` 下推 + pylance 检索 | SQL 搜索表函数（经 namespace） |
| 适用 | 训练数据流水线、RAG、在线 serving | 湖仓 SQL 分析、数仓迁移、Spark ML |

**最佳实践：同一份 Lance 数据，Python 流水线（Ray/pylance）负责训练与 serving，Spark 负责 SQL 批分析**——数据不搬运，各取所长。

## 6.7 最佳实践清单

1. **版本对齐**：按你的 Spark 版本选 `lance-spark-bundle-<spark>_<scala>`，Java 用 11+（17 推荐）。
2. **下推优先**：读时用 `.select()`/`.filter()` 下推到存储层，别在 executors 上全表扫描。
3. **写模式明确**：`append`（追加）/ `overwrite`（覆盖重建，会丢旧版本引用需谨慎）/ `ignore`；upsert 用 `MERGE`。
4. **压实与索引纪律不变**：高频小写后 `compact_files`，压实后重建 ANN 索引（与 pylance 一致）。
5. **版本治理**：Spark 每次写也 +1 版本，配 tags 固定基线 + `cleanup_old_versions` 控磁盘。
6. **Blob V2**：写大对象列用 `file_format_version=2.2` + `lance.encoding=blob`。
7. **对象存储**：`s3://`/`gs://` 配好凭据与生命周期；writer 自动清理需 delete 权限。

## 6.8 验证范围说明

> 本机当前无 Java / Spark 运行时，`examples/19_spark_lance.py` **未在本会话实跑**。文档中的安装坐标、`read/write`/`INSERT`/`MERGE` 语法均来自官方 `lance.org/integrations/spark/` 文档（2026-10 核实）；在你有 Java 11+/Spark 3.5 的环境里按 6.2 的 `--packages` 命令即可运行，跑通后建议把实际输出回填本节。

---

**至此四引擎集成齐了**：pylance（单机）→ Ray（Python 分布式）→ Spark（SQL/JVM 分布式）→ DuckDB（SQL 轻量）。配合入门/精通/实战/运维，Lance 已能覆盖从样本到生产全链路。
