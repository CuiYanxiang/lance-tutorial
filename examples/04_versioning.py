"""04_versioning.py — 精通：versions / time travel / tags / branches。"""
import lance, pyarrow as pa

path = "./versioning.lance"
ds = lance.write_dataset(pa.table({"id": [1, 2], "v": [10, 20]}),
                         path, data_storage_version="2.2", mode="overwrite")
ds.insert(pa.table({"id": [3], "v": [30]}))     # -> version 2
ds.delete("id = 1")                             # -> version 3

print("history:", [v["version"] for v in ds.versions()])

# time travel：读第 1 版快照
v1 = lance.dataset(path, version=1)
print("v1 rows:", v1.count_rows())

# tag：固定基线（不会被 cleanup 回收）
ds.tags.create("baseline", (None, 1))
print("tags:", ds.tags.list())

# branch：并行实验线
ds.create_branch("experiment")
lance.write_dataset(pa.table({"id": [99], "v": [999]}), "experiment", mode="append")
print("experiment latest:", ds.checkout_version(("experiment", None)).to_table().to_pandas())
