#!/usr/bin/env bash
# 打 FaaS 盯盘函数代码包：dist/invest-live-watch.zip（可直接上传阿里云 FC，
# 或把 dist/faas-build/ 目录作为 Serverless Devs 的 code 路径）。
#
# 用法: bash faas/package.sh
# invest-model 代码更新后重跑本脚本再部署即可（持仓快照/投顾信号都走 DB，
# 不随包冻结；只有 watch_etf.txt / trailing_only.txt 这类慢变配置在包里，
# 改动它们后需要重打包）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/dist/faas-build"
rm -rf "$BUILD"
mkdir -p "$BUILD"

echo "→ 安装最小依赖（不含 xgboost/scikit-learn，体积可控）..."
pip install -r "$ROOT/faas/requirements.txt" -t "$BUILD" --quiet

echo "→ 拷贝代码与慢变配置..."
cp -r "$ROOT/invest_model" "$BUILD/"
mkdir -p "$BUILD/scripts" "$BUILD/faas" "$BUILD/config"
cp "$ROOT/scripts/live_check.py" "$BUILD/scripts/"
cp "$ROOT/faas/live_watch_handler.py" "$BUILD/faas/"
# 必带：config.yaml（load_config 硬依赖）；慢变配置按存在拷贝。
# 刻意不带 holding_snapshot_*.csv / advisor_*.csv —— 打包即冻结会过期，
# 函数运行时自动回退查 DB（holding_snapshot / advisor_reco 表，永远最新）。
cp "$ROOT/config/config.yaml" "$BUILD/config/"
for f in watch_etf.txt trailing_only.txt; do
  [ -f "$ROOT/config/$f" ] && cp "$ROOT/config/$f" "$BUILD/config/"
done

( cd "$BUILD" && zip -qr "$ROOT/dist/invest-live-watch.zip" . )
echo "✓ dist/invest-live-watch.zip ($(du -h "$ROOT/dist/invest-live-watch.zip" | cut -f1))"
echo "  本地自测: cd $BUILD && python faas/live_watch_handler.py  （需环境变量就绪）"
