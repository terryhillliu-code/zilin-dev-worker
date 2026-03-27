#!/usr/bin/env python3
import os
import sqlite3
import sys
from pathlib import Path

# v5.9: 优先使用 .pth 配置，回退到 sys.path.insert
try:
    from task_store import TaskStore
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from task_store import TaskStore

def main():
    print("🚀 Phase 6: 自愈闭环压力测试启动 (Self-Healing Stress Test)")
    
    # 注入一个必将失败的任务
    # 指令: 尝试在一个不存在的文件中查找并修改一个不存在的变量
    bad_input = "在 nonexistent_file_999.py 中将变量 GHOST_VARIABLE 修改为 'BUUSTED'。注意：该文件目前不存在，你必须先处理报错。"
    
    store = TaskStore()
    print("\n1. 正在注入恶意/死路任务...")
    
    # 获取模拟任务的模型 (通常由 Router 分配，这里我们手动指定一个 coder 模型)
    task_id = store.enqueue(
        task_input=bad_input,
        initial_status='pending',
        model="qwen3-coder-plus"
    )
    
    print(f"✅ 已注入任务 #{task_id}")
    print("\n预期的执行流:")
    print(f"  1. Worker 认领任务 #{task_id}。")
    print(f"  2. Worker 尝试执行失败（找不到文件）。")
    print(f"  3. Worker 自动触发 CriticAgent 进行复盘。")
    print(f"  4. CriticAgent 生成修复建议，并在 Tasks.db 中自动创建一个新的 [Self-Healing Retry] 任务。")
    
    print("\n--- 验证步骤 ---")
    print(f"1. 运行 Worker: ./test_worker.py")
    print(f"2. 观察日志或运行 `python3 dashboard.py` 查看任务 #{task_id} 的失败情况。")
    print(f"3. 检查 Tasks.db 是否出现了新的带有 'Self-Healing' 标题的任务。")

if __name__ == "__main__":
    main()
