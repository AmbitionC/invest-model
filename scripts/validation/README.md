# 验证 harness（Phase 0：先证明每根杠杆有没有用，过关才实现）

**只读 DB** 的预登记对照实验，产出 go/no-go 报告。对应升级方案「投顾主导 + 量化辅助（验证 + 止盈止损 + 度量投顾战绩）」。

## 跑法
```bash
# 走 .env / INVEST_DB_URL（生产库）
python scripts/validation/run_all.py --out results/validation_report.md
# 或本地 sqlite
python scripts/validation/run_all.py --db sqlite:///./data/local.db
```
云端由 `.github/workflows/validation.yml`（手动触发）在有 prod 凭据 + Tushare 的 runner 上跑，
报告回帖到「🧪 验证报告」issue 并作为 artifact 上传。

## 实验（每个附预登记 H0 + 过关判据）
| 脚本 | 验什么 | 过关判据（简） |
|---|---|---|
| `e1_risk.py` | 持仓级止盈止损（投顾持有制口径） | C 相对不设止损：降大亏/尾部且均值不显著恶化 |
| `e2_advisor.py` | 投顾推荐是否有真实超额 & 分级是否成立 | 分级排序成立且某桶**聚类稳健** \|t\|>2 |
| `e3_validate.py` | 量化「价值陷阱/冲突」标记是否有信息量 | 被标记票后续超额显著更差 |
| `e4_alpha.py` | 因子分位是 alpha 还是小盘 beta | 控制市值后偏 IC 仍稳定为正 |

## 口径铁律
净额（扣成本）、**基准/行业相对超额**、**walk-forward**；投顾推荐扎堆 → 用**聚类稳健 t**（按 主题×rec_date 聚类，
抵消把相关样本当独立导致的显著性高估）。每个实验容错降级：数据不足只标注、不中断整份报告。**全程只读、不改业务逻辑。**
