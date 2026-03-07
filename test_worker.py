#!/usr/bin/env python3
import sys
import time
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path.home() / "zhiwei-dev"))

from worker import Worker

# 创建worker实例并运行
worker = Worker()
print("Worker启动，将持续监听任务...")
try:
    worker.run()
except KeyboardInterrupt:
    print("\nWorker被用户中断")
except Exception as e:
    print(f"Worker异常: {e}")
    import traceback
    traceback.print_exc()