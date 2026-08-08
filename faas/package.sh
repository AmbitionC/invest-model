#!/usr/bin/env bash
# 打 FaaS 定时任务代码包（盯盘/复盘/计划/数据更新/ETF入库 统一一份代码包，
# 供 invest-live-watch 与 invest-scheduler 两个 FC 函数共用）。
#
# 用法: bash faas/package.sh [输出目录]
#   默认输出 dist/faas-build/（目录，作为 Serverless Devs 的 code 路径）
#   并顺手打 dist/invest-faas.zip（控制台手动上传用）。
#   fe-journey-faas 的部署 CI 传入自定义输出目录：bash faas/package.sh "$PWD/invest-dist"
#
# invest-model 代码更新后重跑本脚本再部署即可（持仓快照/投顾信号都走 DB，
# 不随包冻结；只有 watch_etf.txt 这类慢变配置在包里，
# 改动它们后需要重打包）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${1:-$ROOT/dist/faas-build}"
rm -rf "$BUILD"
mkdir -p "$BUILD"

echo "→ 安装最小依赖（不含 xgboost/scikit-learn/pyarrow，体积可控；"
echo "  定时 workflow 与 FaaS 均走 ic 合成模型，不需要 ml 依赖）..."
pip install -r "$ROOT/faas/requirements.txt" -t "$BUILD" --quiet

echo "→ 拷贝代码与慢变配置..."
cp -r "$ROOT/invest_model" "$BUILD/"
mkdir -p "$BUILD/scripts" "$BUILD/faas" "$BUILD/config"
# 各定时任务的 CLI 入口 + 被 faas/jobs.py 直接 import 的辅助脚本。
# 注意：这份清单必须覆盖 jobs.py 里所有 `from scripts.X import ...`——漏带一个，
# 运行时就 ModuleNotFoundError（fear_gauge 曾漏带，导致恐慌落库崩、连累账户快照不落库）。
# 这些脚本只依赖 invest_model + pandas/numpy，均在包内，不引入 ml 重依赖。
for f in live_check.py review.py build_action_plan.py run_pipeline.py ingest_etf_daily.py \
         fear_gauge.py ingest_watermeter.py build_signal_scorecard.py build_arb_scorecard.py \
         ingest_advisor.py ingest_holding_snapshot.py; do
  cp "$ROOT/scripts/$f" "$BUILD/scripts/"
done
cp "$ROOT"/faas/*.py "$BUILD/faas/"
# 必带：config.yaml（load_config 硬依赖）；慢变配置按存在拷贝。
# 刻意不带 holding_snapshot_*.csv / advisor_*.csv —— 打包即冻结会过期，
# 函数运行时自动回退查 DB（holding_snapshot / advisor_reco 表，永远最新）。
cp "$ROOT/config/config.yaml" "$BUILD/config/"
for f in watch_etf.txt; do
  [ -f "$ROOT/config/$f" ] && cp "$ROOT/config/$f" "$BUILD/config/"
done
# 指数基底 CSV（2026-08-08 交叉验证 XV-1 修复）：P26 指数贵贱行、P27 宽基四腿、
# P70 乖离率七指数、P30 盘中 AND 共振全部硬依赖 results/ 下这些 2005 起的全历史基底
# （库内 index_daily 只有 2015 起），且代码全是「文件不存在→静默 return None」——
# 此前不打包＝这一整层在 FC 通道静默失效（17:00 计划缺 P26/P27/P70 行、
# broad_leg_state/index_bias_daily 不落库、盘中 P30 提醒哑火），只有 Actions bump
# 通道（全量 checkout）能出完整计划，故一直没暴露。共 ~700K，体积可忽略。
mkdir -p "$BUILD/results"
for f in index_dump_000300_SH.csv index_dump_000688_SH.csv index_dump_000922_CSI.csv \
         index_dump_000016_SH.csv index_dump_000905_SH.csv index_dump_000852_SH.csv \
         spread_full_history.csv; do
  if [ -f "$ROOT/results/$f" ]; then
    cp "$ROOT/results/$f" "$BUILD/results/"
  else
    echo "⚠️ 缺基底 CSV results/$f（该腿提示层将静默跳过）" >&2
  fi
done

if [ "$BUILD" = "$ROOT/dist/faas-build" ]; then
  ( cd "$BUILD" && zip -qr "$ROOT/dist/invest-faas.zip" . )
  echo "✓ dist/invest-faas.zip ($(du -h "$ROOT/dist/invest-faas.zip" | cut -f1))"
fi
echo "✓ 代码包目录: $BUILD ($(du -sh "$BUILD" | cut -f1))"
echo "  本地自测: cd $BUILD && python faas/scheduler_handler.py <job>  （需环境变量就绪）"
