"""09_rag.py — 实战：轻量 RAG：写 embedding 列 + IVF-PQ + FTS，两路召回。
注意：embedding 列必须是定长 FixedSizeList。
"""
import lance, pyarrow as pa, random

random.seed(1)
DIM = 8
chunks = ["lance is a lakehouse format", "it supports vector search",
          "random access is fast", "bm25 full text search built in"]
vec_type = pa.list_(pa.float32(), DIM)

table = pa.table({
    "id":        list(range(len(chunks))),
    "text":      chunks,
    "embedding": pa.array([[random.gauss(0, 1) for _ in range(DIM)] for _ in chunks],
                          type=vec_type),
})
ds = lance.write_dataset(table, "./rag.lance", data_storage_version="2.2",
                         mode="overwrite")
ds.create_index("embedding", index_type="IVF_PQ", metric="cosine",
                num_partitions=8, num_sub_vectors=4)
ds.create_scalar_index("text", index_type="INVERTED")

q = [random.gauss(0, 1) for _ in range(DIM)]
print("vector hits:", ds.to_table(nearest={"column": "embedding", "q": q, "k": 2},
                                 columns=["text"]).to_pandas()["text"].tolist())
print("keyword hits:", ds.to_table(full_text_query="vector", columns=["text"],
                                   limit=2).to_pandas()["text"].tolist())
