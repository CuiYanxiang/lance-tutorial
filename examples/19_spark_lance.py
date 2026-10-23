"""19_spark_lance.py — Spark + Lance 集成示例（DSv2）。
运行前置（有 Java 11+ 与 Spark 3.5 的环境）：
    spark-submit --packages org.lance:lance-spark-bundle-3.5_2.12:0.4.0 examples/19_spark_lance.py
本机若未检测到 pyspark/Java，脚本会打印指引并退出（不模拟执行）。

依赖：uv add "pyspark==3.5.*"
"""
import sys

try:
    from pyspark.sql import SparkSession
except ImportError:
    print("跳过：未安装 pyspark。请先：uv add \"pyspark==3.5.*\"")
    print("然后：spark-submit --packages org.lance:lance-spark-bundle-3.5_2.12:0.4.0 examples/19_spark_lance.py")
    sys.exit(0)

spark = (SparkSession.builder
         .appName("lance-spark-demo")
         .master("local[*]")
         .config("spark.jars.packages", "org.lance:lance-spark-bundle-3.5_2.12:0.4.0")
         .getOrCreate())

# 1) 造数据并写入 Lance（DSv2 data source）
df = spark.range(0, 1000).selectExpr(
    "id",
    "id * 2 as score",
    "concat('user_', id % 100) as name",
)
df.write.format("lance").mode("overwrite").save("./spark_ds.lance")

# 2) 读回：列裁剪 + 过滤下推
read = (spark.read.format("lance")
        .option("batch_size", "4096")
        .load("./spark_ds.lance")
        .select("id", "name")
        .filter("score > 1500"))
print("filtered count:", read.count())
read.show(5)

# 3) 注册为 SQL 表做分析
spark.read.format("lance").load("./spark_ds.lance").createOrReplaceTempView("t")
spark.sql("SELECT name, COUNT(*) c FROM t GROUP BY name ORDER BY c DESC LIMIT 5").show()

spark.stop()
print("Spark + Lance OK")
