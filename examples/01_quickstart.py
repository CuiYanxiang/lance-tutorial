"""01_quickstart.py — 入门：建表 + 投影/过滤 + 随机取行。
运行：python 01_quickstart.py
"""
import lance, pyarrow as pa

# 1) 写（data_storage_version 固定 2.2）
tbl = pa.table({
    "id":   [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Carla", "David", "Edgar"],
    "age":  [20, 30, 37, 42, 46],
})
ds = lance.write_dataset(tbl, "./demo.lance", data_storage_version="2.2", mode="overwrite")
print("version:", ds.version, "rows:", ds.count_rows())

# 2) 读：只取 name 列 + 下推过滤
print(ds.to_table(columns=["name"], filter="age >= 30").to_pandas())

# 3) 随机取第 0 / 2 / 4 行（Lance 的杀手锏）
print(ds.take([0, 2, 4], columns=["name", "age"]).to_pandas())

# 4) 随机采样 3 行
print(ds.sample(3, columns=["name"]).to_pandas())
