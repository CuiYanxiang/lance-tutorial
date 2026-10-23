"""03_merge_upsert.py — 精通：merge_insert 的四种语义。"""
import lance, pyarrow as pa

ds = lance.write_dataset(
    pa.table({"name": ["Alice", "Bob"], "age": [20, 30]}),
    "./merge.lance", data_storage_version="2.2", mode="overwrite",
)

# ① Upsert：命中更新，未命中插入
updates = pa.table({"name": ["Alice", "Carla"], "age": [25, 37]})
ds.merge_insert("name").when_matched_update_all().when_not_matched_insert_all().execute(updates)

# ② 不存在才插入（幂等导入）：Bob 已在，不变；Fran 新增
new = pa.table({"name": ["Bob", "Fran"], "age": [30, 44]})
ds.merge_insert("name").when_not_matched_insert_all().execute(new)

print(ds.to_table().to_pandas())
