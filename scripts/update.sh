#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/taojk"
branch="${1:-main}"
remote="${2:-origin}"

if [ -d "$APP_DIR/.git" ]; then
  cd "$APP_DIR"
else
  cd "$(dirname "$0")/.."
fi

echo "正在强制同步 $remote/$branch ..."
git fetch "$remote" "$branch"
git reset --hard "$remote/$branch"

echo "正在安装/更新 Python 依赖..."
if [ ! -d "venv" ]; then
  python3 -m venv venv
fi
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt

if systemctl list-unit-files taomonitor.service >/dev/null 2>&1; then
  echo "正在重启 taomonitor 服务..."
  systemctl daemon-reload
  systemctl restart taomonitor
else
  echo "未检测到 taomonitor.service。如是首次安装，请运行 ./install.sh。"
fi

echo "当前版本：$(git rev-parse --short HEAD)"
echo "更新完成。"
