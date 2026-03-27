#!/bin/bash
# 知微大脑直连工具 (Brain Cmd Bridge)
# 使宿主机 AI 能够轻松操控容器内的大脑

TASK=$1
if [ -z "$TASK" ]; then
    echo "Usage: $0 \"<task_description>\""
    exit 1
fi

# 核心路径与环境变量同步
WORKSPACE="/root/workspace"
CONTAINER="clawdbot"

# 检查容器状态
if ! docker ps --format '{{.Names}}' | grep -q "^$CONTAINER$"; then
    echo "❌ 错误: 容器 $CONTAINER 未运行。请先运行 'docker restart $CONTAINER'。"
    exit 1
fi

echo "🧠 正在连接知微大脑 (Docker: $CONTAINER)..."

# 执行容器内 Claude Code (使用 stdin 传入任务)
docker exec \
    --user 1000:1000 \
    -e HOME=/home/node \
    -e ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
    -e TERM=xterm \
    -e COLUMNS=100 \
    -w "$WORKSPACE" \
    "$CONTAINER" \
    bash -c "echo '$TASK' | npx -y @anthropic-ai/claude-code --dangerously-skip-permissions --allowedTools Edit,Write,Read,Terminal,Search --print"
