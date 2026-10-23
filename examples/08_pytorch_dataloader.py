"""08_pytorch_dataloader.py — 实战：Lance 数据集喂 PyTorch DataLoader（随机取行飞快）。
依赖：uv add torch
"""
import lance, pyarrow as pa
from torch.utils.data import Dataset, DataLoader

# 造一个内存数据集：id + 16 维向量 + label
rows = [{"id": i, "vec": [float(i % 7)] * 16, "label": i % 3} for i in range(500)]
lance.write_dataset(pa.Table.from_pylist(rows), "./train.lance",
                    data_storage_version="2.2", mode="overwrite")


class LanceDS(Dataset):
    def __init__(self, uri):
        self.ds = lance.dataset(uri)
    def __len__(self):
        return self.ds.count_rows()
    def __getitem__(self, idx):
        row = self.ds.take([idx], columns=["vec", "label"]).to_pylist()[0]
        import numpy as np
        return np.array(row["vec"], dtype="float32"), row["label"]


if __name__ == "__main__":
    loader = DataLoader(LanceDS("./train.lance"), batch_size=32, shuffle=True)
    x, y = next(iter(loader))
    print("batch shape:", x.shape, "labels:", y[:5].tolist())
