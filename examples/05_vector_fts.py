"""05_vector_fts.py — 精通：IVF-PQ 向量索引 + FTS + 混合检索。
注意：向量列必须是定长 FixedSizeList，不能是变长 list<double>。
"""
import lance, pyarrow as pa, random

random.seed(0)
N, DIM = 2000, 16
vec_type = pa.list_(pa.float32(), DIM)          # 定长向量类型

table = pa.table({
    "id":    list(range(N)),
    "text":  [f"doc {i} about machine learning and retrieval" for i in range(N)],
    "vector": pa.array([[random.gauss(0, 1) for _ in range(DIM)] for _ in range(N)],
                       type=vec_type),
})
ds = lance.write_dataset(table, "./vec.lance", data_storage_version="2.2",
                         mode="overwrite")

# 建索引（生产按 sqrt(N) 调 num_partitions）
ds.create_index("vector", index_type="IVF_PQ", metric="cosine",
                num_partitions=32, num_sub_vectors=8)
# 全文/倒排是「标量索引」，用 create_scalar_index，不是 create_index
ds.create_scalar_index("text", index_type="INVERTED")   # BM25 全文

q = [random.gauss(0, 1) for _ in range(DIM)]
print("KNN:", ds.to_table(nearest={"column": "vector", "q": q, "k": 3},
                          columns=["id"]).to_pandas()["id"].tolist())
print("FTS:", ds.to_table(full_text_query="retrieval", columns=["id"],
                          limit=3).to_pandas()["id"].tolist())
