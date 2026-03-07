"""Claude Code 执行后端"""

import subprocess
import os
from pathlib import Path

from .base import DevBackend, ExecuteResult


class ClaudeCodeBackend(DevBackend):

    def __init__(self, model: str = "qwen3-coder-plus",
                 allowed_tools: str = "Edit,Write,Read",
                 timeout: int = 300):
        self.model = model
        self.allowed_tools = allowed_tools
        self.timeout = timeout

    @property
    def name(self) -> str:
        return "claude_code"

    def execute(self, task: str, workspace: str, log_path: str, model_override: str = None) -> ExecuteResult:
        """执行开发任务"""
        target_model = model_override or self.model
        cmd = [
            "claude", "-p", task,
            "--model", target_model,
            "--allowedTools", self.allowed_tools,
        ]

        # 确保工作目录存在
        workspace = os.path.expanduser(workspace)
        if not os.path.exists(workspace):
            return ExecuteResult(
                success=False,
                stdout="",
                stderr=f"Workspace not found: {workspace}",
                returncode=1
            )

        try:
            # 执行命令
            result = subprocess.run(
                cmd,
                cwd=workspace,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                env={**os.environ, "CLAUDECODE": ""}  # 清除嵌套标记
            )

            # 写入日志
            log_path = os.path.expanduser(log_path)
            with open(log_path, "w") as f:
                f.write(f"=== COMMAND ===\n{' '.join(cmd)}\n\n")
                f.write(f"=== CWD ===\n{workspace}\n\n")
                f.write(f"=== STDOUT ===\n{result.stdout}\n\n")
                f.write(f"=== STDERR ===\n{result.stderr}\n\n")
                f.write(f"=== RETURNCODE ===\n{result.returncode}\n")

            return ExecuteResult(
                success=result.returncode == 0,
                stdout=result.stdout,
                stderr=result.stderr,
                returncode=result.returncode
            )

        except subprocess.TimeoutExpired:
            return ExecuteResult(
                success=False,
                stdout="",
                stderr=f"Timeout after {self.timeout}s",
                returncode=-1
            )
        except Exception as e:
            return ExecuteResult(
                success=False,
                stdout="",
                stderr=str(e),
                returncode=-1
            )


# 测试代码
if __name__ == "__main__":
    backend = ClaudeCodeBackend()

    # 创建测试目录
    test_dir = "/tmp/claude-backend-test"
    os.makedirs(test_dir, exist_ok=True)

    result = backend.execute(
        task="创建 test.py，内容为 print('backend test ok')",
        workspace=test_dir,
        log_path=f"{test_dir}/run.log"
    )

    print(f"Success: {result.success}")
    print(f"Stdout: {result.stdout[:200]}")

    # 验证文件
    test_file = f"{test_dir}/test.py"
    if os.path.exists(test_file):
        print(f"File content: {open(test_file).read()}")
    else:
        print("File not created!")