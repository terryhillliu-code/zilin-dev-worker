#!/bin/bash
# tasks.db 定期备份脚本
BACKUP_DIR=~/zhiwei-dev/db_backups
mkdir -p "$BACKUP_DIR"
DATE=$(date +%Y%m%d_%H%M%S)
cp ~/zhiwei-dev/tasks.db "$BACKUP_DIR/tasks_${DATE}.db"
# 只保留最近 10 个备份
ls -t "$BACKUP_DIR"/tasks_*.db | tail -n +11 | xargs rm -f 2>/dev/null
echo "备份完成: $BACKUP_DIR/tasks_${DATE}.db"
