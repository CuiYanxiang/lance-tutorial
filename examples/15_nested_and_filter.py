"""15_nested_and_filter.py — 进阶案例：嵌套类型（struct/list）+ 复杂 filter 下推。"""
import lance, pyarrow as pa

# struct 字段：note；list 字段：tags
rows = [
    {"id": 1, "note": {"email": "a@x.com", "created": True}, "tags": ["ai", "ml"], "score": 90},
    {"id": 2, "note": {"email": None,   "created": False}, "tags": ["db"],    "score": 40},
    {"id": 3, "note": {"email": "c@x.com", "created": True}, "tags": ["ai", "db"], "score": 70},
]
ds = lance.write_dataset(pa.Table.from_pylist(rows), "./nested.lance",
                         data_storage_version="2.2", mode="overwrite")

# struct 用字段名下标，list 用序号下标；AND/OR/NOT 组合
tbl = ds.to_table(
    filter="(note['email'] IS NOT NULL AND note['created'] = true) AND score >= 70"
)
print(tbl.to_pandas())

# list 元素下标访问 + IN
print(ds.to_table(filter="tags[0] = 'ai'").to_pandas())

# 正则 + LIKE
print(ds.to_table(filter="regexp_match(note['email'], '.*@x\\.com$')").to_pandas())
