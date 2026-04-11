import os
import subprocess
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DockerSandbox:
    """
    Docker 容器化沙箱
    用于在隔离容器中执行 AI 开发任务，保护宿主机安全。
    """
    def __init__(self, image: str = "python:3.11-slim"):
        self.image = image
        self._ensure_image()

    def _ensure_image(self):
        """检查并确保镜像存在"""
        try:
            subprocess.run(["docker", "image", "inspect", self.image], 
                           check=True, capture_output=True)
        except subprocess.CalledProcessError:
            logger.info(f"🚚 正在拉取沙箱镜像: {self.image}...")
            subprocess.run(["docker", "pull", self.image], check=True)

    def run_task(self, task_input: str, workspace_path: str, log_path: str, timeout: int = 600):
        """
        在 Docker 容器中执行任务
        """
        workspace_path = os.path.abspath(workspace_path)
        log_path = os.path.abspath(log_path)
        container_name = f"zhiwei-sandbox-{os.path.basename(workspace_path)}"

        # 构造 Docker 命令
        # 我们需要将宿主机的 workspace 挂载到容器内的 /workspace
        # 并且需要传递环境变量（如 ANTHROPIC_AUTH_TOKEN）
        cmd = [
            "docker", "run", "--rm",
            "--name", container_name,
            "-v", f"{workspace_path}:/workspace",
            "-w", "/workspace",
            "-e", f"ANTHROPIC_AUTH_TOKEN={os.environ.get('ANTHROPIC_AUTH_TOKEN', '')}",
            "-e", "CLAUDECODE=1",  # 告知内部是在知微环境下
            self.image,
            "bash", "-c", 
            f"echo 'Running AI Task...' && {self._generate_exec_script(task_input)}"
        ]

        logger.info(f"🚀 启动容器化执行: {container_name}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            # 记录日志
            with open(log_path, "w") as f:
                f.write(f"=== DOCKER STDOUT ===\n{result.stdout}\n")
                f.write(f"=== DOCKER STDERR ===\n{result.stderr}\n")
                f.write(f"=== RETURNCODE ===\n{result.returncode}\n")
            
            return {
                "success": result.returncode == 0,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "returncode": result.returncode
            }
            
        except subprocess.TimeoutExpired:
            logger.error(f"❌ 容器执行超时 ({timeout}s)")
            subprocess.run(["docker", "kill", container_name], capture_output=True)
            return {"success": False, "stderr": "Timeout", "returncode": -1}
        except Exception as e:
            logger.error(f"❌ 容器执行异常: {e}")
            return {"success": False, "stderr": str(e), "returncode": -1}

    def _generate_exec_script(self, task_input: str) -> str:
        """
        在容器内安装必要工具并执行。
        注意：实际生产中建议使用预装了 claude 的专用镜像。
        这里为了演示，我们在运行时尝试安装（非最高效，但最通用）。
        """
        # 逃逸字符处理
        safe_task = task_input.replace("'", "'\"'\"'")
        
        # 容器内逻辑：安装 curl -> 安装 claude -> 执行
        script = f"""
        apt-get update -qq && apt-get install -y -qq curl git > /dev/null
        # 假设我们通过 npm 或其他方式获取 claude
        # 这里简化为直接执行一个模拟成功或安装命令
        # 提示：由于 claude CLI 比较大，真实环境应在镜像中预载
        curl -fsSL https://raw.githubusercontent.com/anthropic/claude-code/main/install.sh | sh -s -- -y > /dev/null 2>&1 || true
        
        # 执行任务
        if command -v claude >/dev/null 2>&1; then
            claude -p '{safe_task}' --model qwen3-coder-plus
        else
            echo "错误: 容器内未找到 claude 指令。请确保镜像预装了 claude-code。"
            exit 1
        fi
        """
        return " && ".join([line.strip() for line in script.split("\n") if line.strip()])

if __name__ == "__main__":
    # 简单测试
    logging.basicConfig(level=logging.INFO)
    sandbox = DockerSandbox()
    res = sandbox.run_task("创建 README.md", "/tmp/test-ws", "/tmp/test-run.log")
    print(res)
