#!/bin/bash
# TAO 监控系统安装脚本
# 针对 PEP 668 (Ubuntu 22.04+/24.04) 进行了兼容性优化

echo "=========================================="
echo "        TAO 监控系统 - 安装向导        "
echo "=========================================="

if [ "$EUID" -ne 0 ]; then
  echo "❌ 请使用 root 权限运行此脚本"
  exit 1
fi

APP_DIR="/opt/taojk"
SERVICE_FILE="/etc/systemd/system/taomonitor.service"

ask_port() {
    local default_port="$1"
    read -p "网页访问端口 (默认 ${default_port}): " PORT
    PORT=${PORT:-$default_port}
}

ask_admin() {
    read -p "管理员账号: " ADMIN_USER
    read -p "管理员密码: " ADMIN_PASS
}

ask_reset_admin() {
    read -p "是否重置管理员账号密码？(y/N): " RESET_ADMIN
    if [ "$RESET_ADMIN" = "y" ] || [ "$RESET_ADMIN" = "Y" ]; then
        ask_admin
    fi
}

get_existing_port() {
    if [ -f "$SERVICE_FILE" ]; then
        grep -Eo 'PORT=[0-9]+' "$SERVICE_FILE" | head -n 1 | cut -d'=' -f2
    fi
}

# 基础依赖安装
echo "⚙️ 安装/检查系统依赖..."
apt update && apt install -y python3 python3-venv python3-pip git sqlite3

# 1. 判断安装模式。新服务器 git clone 到 /opt/taojk 后，目录会存在，但服务还不存在，仍应走全新安装。
if [ ! -f "$SERVICE_FILE" ]; then
    echo "🆕 执行【全新安装模式】..."
    ask_port "8000"
    ask_admin

    CURRENT_DIR="$(pwd)"
    if [ "$CURRENT_DIR" != "$APP_DIR" ]; then
        rm -rf "$APP_DIR"
        mkdir -p "$APP_DIR"
        cp -r . "$APP_DIR"
    fi
else
    echo "♻️ 执行【更新模式】..."
    cd "$APP_DIR"
    if [ -x "scripts/update.sh" ]; then
        ./scripts/update.sh
    else
        git fetch origin main
        git reset --hard origin/main
    fi
    EXISTING_PORT=$(get_existing_port)
    ask_port "${EXISTING_PORT:-8000}"
    ask_reset_admin
fi

cd "$APP_DIR"

# 2. 虚拟环境强制重建/检查 (核心修复逻辑)
if [ ! -d "venv" ]; then
    echo "🐍 创建 Python 虚拟环境..."
    python3 -m venv venv
fi

echo "📦 安装/更新 Python 依赖..."
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt

# 3. 初始化数据库
if [ -z "$ADMIN_USER" ]; then
    USER_COUNT=$(sqlite3 data.db "SELECT count(*) FROM users;" 2>/dev/null || echo "0")
    if [ "$USER_COUNT" = "0" ]; then
        echo "还没有管理员账号，请先创建一个。"
        ask_admin
    fi
fi

if [ ! -z "$ADMIN_USER" ]; then
    echo "🗄️ 初始化数据库及账号..."
    ADMIN_USER="$ADMIN_USER" ADMIN_PASS="$ADMIN_PASS" ./venv/bin/python3 -c "
import os
import database as db
db.init_db()
db.create_admin_user(os.environ['ADMIN_USER'], os.environ['ADMIN_PASS'])
"
fi

# 4. 配置并重启系统服务
echo "📝 配置系统服务 (Port: $PORT)..."
SESSION_SECRET=$(python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(48))
PY
)
cat <<EOF > "$SERVICE_FILE"
[Unit]
Description=TAO PRO Monitor Service
After=network.target

[Service]
User=root
WorkingDirectory=$APP_DIR
Environment="PORT=$PORT"
Environment="SESSION_SECRET=$SESSION_SECRET"
ExecStart=$APP_DIR/venv/bin/python app.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable taomonitor
systemctl restart taomonitor

echo "=========================================="
echo "安装完成"
echo "访问地址: http://$(hostname -I | awk '{print $1}'):$PORT"
echo "=========================================="
