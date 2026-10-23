"""17_take_sample_serving.py — 进阶案例：随机访问 / 采样在 serving 与训练的应用。
"""
import lance, pyarrow as pa

rows = [{"id": i, "vec": [float(i % 5)] * 4, "label": i % 3} for i in range(1000)]
ds = lance.write_dataset(pa.Table.from_pylist(rows), "./serving.lance",
                         data_storage_version="2.2", mode="overwrite")

# 1) 点查：按行号随机取（serving / 交互式取单条）
print(ds.take([7, 42, 999], columns=["id", "label"]).to_pandas())

# 2) 采样：不按顺序、可重复 seed（训练 shuffle / 抽检）
sample1 = ds.sample(50, columns=["id"])
sample2 = ds.sample(50, columns=["id"])
print("sample order differs:", (sample1.column("id").to_pylist() != sample2.column("id").to_pylist()))

# 3) 高效取 blob：take_blobs 一次取多行二进制。注意列必须是 Blob V2 列
#    （lance.blob_field 返回 Field、lance.blob_array 直接从 bytes 构造），普通 binary 不行。
schema = pa.schema([pa.field("id", pa.int64()), lance.blob_field("img")])
blob_ds = lance.write_dataset(
    pa.Table.from_arrays(
        [pa.array([0, 1, 2]),
         lance.blob_array([b"\x89PNG-a", b"\x89PNG-b", b"\x89PNG-c"])],
        schema=schema),
    "./blobs2.lance", data_storage_version="2.2", mode="overwrite")
# take_blobs 的 ids 需要「行地址」（row address），用 with_row_address=True 读出
addrs = next(blob_ds.to_batches(with_row_address=True)).column("_rowaddr").to_pylist()
blobs = blob_ds.take_blobs("img", ids=[addrs[0], addrs[2]])
print("blobs count:", len(blobs), "first size:", blobs[0].size)
