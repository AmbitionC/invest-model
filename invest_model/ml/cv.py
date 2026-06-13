"""时序 walk-forward 交叉验证。

为防止前瞻泄露：
1. 严格按时间顺序切分（不打乱）
2. 训练集与验证集之间留 embargo 间隔（>= max horizon），
   因为 forward_return 标签需要未来数据，临近的训练样本可能与验证集重叠
3. 滚动 N 折，每折训练窗口固定大小，验证窗口紧随其后
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

import numpy as np


@dataclass
class CvFold:
    """单个交叉验证折。"""
    fold_id: int
    train_idx: np.ndarray  # 训练集索引（基于原始升序数据）
    val_idx: np.ndarray    # 验证集索引


@dataclass
class PurgedWalkForwardSplit:
    """时序 walk-forward 划分器。

    Parameters
    ----------
    n_splits : int
        滚动折数
    train_window : int
        每折训练样本数
    val_window : int
        每折验证样本数
    embargo : int
        训练集与验证集之间的间隔样本数（应 >= max horizon）
    """

    n_splits: int = 5
    train_window: int = 500
    val_window: int = 60
    embargo: int = 10

    def split(self, n_samples: int) -> Iterator[CvFold]:
        """对长度为 n_samples 的时序数据生成折。

        策略：从最末端往前安排折，确保最后一折用最新数据评估。
        """
        if n_samples <= 0:
            return

        # 实际可用的最末位置
        end = n_samples
        folds: list[CvFold] = []

        for k in range(self.n_splits):
            val_end = end - k * self.val_window
            val_start = val_end - self.val_window
            train_end = val_start - self.embargo
            train_start = max(0, train_end - self.train_window)

            if val_start < 0 or train_end <= train_start:
                break
            if val_end > n_samples or train_end > val_start - self.embargo + 1:
                # 防御性检查
                pass

            train_idx = np.arange(train_start, train_end)
            val_idx = np.arange(val_start, val_end)
            if len(train_idx) < 50 or len(val_idx) < 5:
                break
            folds.append(CvFold(
                fold_id=self.n_splits - 1 - k,
                train_idx=train_idx,
                val_idx=val_idx,
            ))

        # 按时间正序输出
        for f in sorted(folds, key=lambda x: x.fold_id):
            yield f

    def final_train_range(self, n_samples: int) -> tuple[int, int]:
        """返回最终生产模型的训练区间 [start, end)。

        生产模型用全量历史训练（含原本作为最后一折验证集的部分），
        但仍需保留尾部 embargo 个样本（这些样本的标签未必完整）。
        """
        end = max(0, n_samples - self.embargo)
        start = max(0, end - self.train_window)
        return start, end
