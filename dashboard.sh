#!/bin/bash
# 知微任务看板一键启动脚本 (v5.6.7 Robust)
VENV_PYTHON="/Users/liufang/zhiwei-bot/venv/bin/python3"
DASH_SCRIPT="/Users/liufang/zhiwei-dev/dashboard.py"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Error: Virtual environment python not found at $VENV_PYTHON"
    exit 1
fi

if [ ! -f "$DASH_SCRIPT" ]; then
    echo "❌ Error: Dashboard script not found at $DASH_SCRIPT"
    exit 1
fi

# Execute with any passed arguments
$VENV_PYTHON $DASH_SCRIPT "$@"