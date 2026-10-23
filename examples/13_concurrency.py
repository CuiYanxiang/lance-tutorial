"""13_concurrency.py — 运维：并发写的乐观锁冲突与重试范式。"""
import lance, pyarrow as pa

path = "./concurrent.lance"
lance.write_dataset(pa.table({"id": [1], "v": [10]}), path,
                    data_storage_version="2.2", mode="overwrite")


def safe_write(table, retries=3):
    """并发写时 manifest 版本冲突会失败，必须捕获重试。"""
    for attempt in range(retries):
        try:
            lance.write_dataset(table, path, mode="append")
            return True
        except Exception as e:
            print(f"attempt {attempt + 1} conflict: {type(e).__name__}; retrying...")
    raise RuntimeError("写冲突重试耗尽")


# 模拟两路并发 append（真实场景在多线程/多进程里）
safe_write(pa.table({"id": [2], "v": [20]}))
safe_write(pa.table({"id": [3], "v": [30]}))
print("final rows:", lance.dataset(path).count_rows())
