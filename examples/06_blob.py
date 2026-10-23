"""06_blob.py — 精通：Blob 懒加载（用内存里的小字节块模拟图片二进制）。"""
import lance, pyarrow as pa

# 用 binary 列存小 blob（真实场景是图片/视频字节；2.2 可用 Blob V2 外置）
rows = [{"id": i, "img_bytes": b"\x89PNG fake-bytes-" + str(i).encode()}
        for i in range(5)]
ds = lance.write_dataset(pa.Table.from_pylist(rows), "./blob.lance",
                         data_storage_version="2.2", mode="overwrite")

# 随机取行拿到 blob
print(ds.take([0, 3], columns=["id", "img_bytes"]).to_pandas())
