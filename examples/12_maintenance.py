"""12_maintenance.py — 运维：compact + cleanup_old_versions + 自动清理。"""
import lance, pyarrow as pa
from datetime import timedelta
from lance.dataset import AutoCleanupConfig

path = "./maint.lance"
ds = lance.write_dataset(pa.table({"id": [1, 2], "v": [10, 20]}),
                         path, data_storage_version="2.2", mode="overwrite")

# 制造一堆小片段 + 软删
for i in range(10):
    ds.insert(pa.table({"id": [100 + i], "v": [i]}))
ds.delete("id < 3")

# 压实：合并小片段、物理清除软删行（压实后若建了 ANN 索引需重建）
ds.optimize.compact_files(target_rows_per_fragment=1024 * 1024,
                          data_storage_version="2.2")

# 清理 1 天前的旧版本（回收磁盘；打了 tag 的版本豁免）
ds.cleanup_old_versions(older_than=timedelta(days=1))

# 也可开启写路径内自动清理（高并发生产更推荐改后台 cron）
ds.optimize.enable_auto_cleanup(AutoCleanupConfig(interval=20, older_than_seconds=3600))
print("versions now:", [v["version"] for v in lance.dataset(path).versions()])
