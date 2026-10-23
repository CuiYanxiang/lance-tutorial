"""11_hf_hub.py — 实战：从 HuggingFace Hub 直接流式读 Lance 数据集。
需要 huggingface_hub 已登录且网络可达 HF。
"""
import lance

try:
    ds = lance.dataset("hf://datasets/lance-format/laion-1m/data/train.lance")
    print("rows:", ds.count_rows())
    print(ds.to_table(limit=2).to_pandas().head())
except Exception as e:  # 网络/凭据不可达时给出提示而不崩
    print("跳过：无法访问 HF Hub。原因:", type(e).__name__, e)
