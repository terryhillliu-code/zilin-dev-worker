#!/usr/bin/env python3
import os
import subprocess
import sys
import time
from pathlib import Path

def main():
    print("🚀 Phase 6: 流水线全链路验证启动 (Complex Pipeline Test)")
    print("目标需求: 生成一份包含 tasks.db 统计与 RAG 技术要点的上周开发总结，存入 reports/weekly.md")
    
    request = "生成一份包含 tasks.db 统计与 RAG 技术要点的上周开发总结，存入 reports/weekly.md"
    
    # 1. 触发编排
    orchestrator_path = Path(__file__).parent / "orchestrator.py"
    print(f"\n1. 正在调用编排器: {orchestrator_path}")
    
    env = os.environ.copy()
    if "ANTHROPIC_AUTH_TOKEN" not in env:
        print("❌ 错误: 未设置 ANTHROPIC_AUTH_TOKEN 环境变量")
        sys.exit(1)
        
    result = subprocess.run(
        [sys.executable, str(orchestrator_path), request],
        capture_output=True,
        text=True,
        env=env
    )
    
    print(result.stdout)
    if result.returncode != 0:
        print(f"❌ 编排失败: {result.stderr}")
        sys.exit(1)
        
    print("\n✅ 编排任务已入库。请确保 Worker 正在运行以处理任务。")
    print("提示: 您可以运行 `./start_worker.sh` 来启动 Worker。")
    print("监控命令: python3 dashboard.py")

if __name__ == "__main__":
    main()
