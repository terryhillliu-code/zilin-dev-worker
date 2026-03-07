#!/bin/bash
cd "$(dirname "$0")"

# 加载环境变量
export FEISHU_USER_ID="这里填你的飞书 user_id"

VENV_PATH="$HOME/zhiwei-scheduler/venv/bin/python"

if [ ! -f "$VENV_PATH" ]; then
    echo "创建 venv..."
    python3 -m venv venv
    VENV_PATH="./venv/bin/python"
fi

echo "启动 Worker..."
exec "$VENV_PATH" worker.py