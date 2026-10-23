"""02_crud.py — 入门：insert / delete / update，理解「每次写都 +1 版本」。"""
import lance, pyarrow as pa

ds = lance.write_dataset(
    pa.table({"name": ["Alice", "Bob"], "age": [20, 30]}),
    "./crud.lance", data_storage_version="2.2", mode="overwrite",
)

ds.insert(pa.table({"name": ["Carla"], "age": [37]}))          # append
print("after insert -> version", ds.version)

ds.delete("name = 'Bob'")                                       # 软删
ds.update({"age": "age + 2"})                                   # SQL 表达式更新

# 关键：改完要重新打开才看到最新
ds = lance.dataset("./crud.lance")
print(ds.to_table().to_pandas())
