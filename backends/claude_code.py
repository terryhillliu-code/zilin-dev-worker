"""OpenClaw 执行后端 (Dockerized)"""

import subprocess
import os
from pathlib import Path

from .base import DevBackend, ExecuteResult


class ClaudeCodeBackend(DevBackend):
    """
    OpenClaw 执行后端 (Dockerized)
    遵循 AGENTS.md: Host 为 Limb, Docker (clawdbot) 为 Brain.
    """

    def __init__(self, agent_id: str = "main",
                 timeout: int = 1200):
        self.agent_id = agent_id
        self.timeout = timeout
        
        # 引入消息总线用于执行中的心跳反馈
        try:
            from message_bus import MessageBus
            self.msg_bus = MessageBus()
        except:
            self.msg_bus = None

    @property
    def name(self) -> str:
        return "openclaw_docker"

    def execute(self, task: str, workspace: str, log_path: str, 
                model_override: str = None, retry_context: str = None,
                bypass_prompts: bool = False) -> ExecuteResult:
        """在 Docker 容器内执行开发/研究任务"""
        
        # 1. 路径转换 (Host -> Container)
        # Host: /Users/liufang/clawdbot-docker/workspace/tasks/task-42
        # Container: /root/workspace/tasks/task-42
        container_workspace = workspace.replace("/Users/liufang/clawdbot-docker/workspace", "/root/workspace")
        
        # 2. 注入自愈上下文
        final_task = task
        if retry_context:
            final_task = f"【自愈修复指令】\n上一次执行验证失败。错误输出如下：\n---\n{retry_context}\n---\n请分析失败原因并修正。原始需求是：\n{task}"

        # 3. 构造 Docker 命令
        # 使用容器内安装的 claude (Claude Code) CLI
        # 注入宿主机的 API Key 确保认证通过
        dashscope_key = os.environ.get("DASHSCOPE_API_KEY", "")
        qwen_key = os.environ.get("QWEN_API_KEY", "")
        anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

        # v5.8: 支持百炼 API (Anthropic 兼容端点)
        anthropic_base_url = os.environ.get("ANTHROPIC_BASE_URL", "")
        anthropic_auth_token = os.environ.get("ANTHROPIC_AUTH_TOKEN", "")
        anthropic_model = os.environ.get("ANTHROPIC_MODEL", "glm-5")

        # v5.6.1: 核心变更 - 在大脑容器内执行 Chief Engineer 任务
        # 使用 npx -y 确保即便没有全局安装也能直接运行
        # v5.7: 修复 --print 模式，通过 stdin 传入任务
        # 使用 bash -c 封装以获得更好的参数处理稳定性
        task_safe = final_task.replace("'", "'\"'\"'")  # 单引号转义

        inner_cmd = f"echo '{task_safe}' | npx -y @anthropic-ai/claude-code --dangerously-skip-permissions --allowedTools Edit,Write,Read,Terminal,Search --print"
        
        cmd = [
            "docker", "exec",
            "--user", "1000:1000",
            "-e", "HOME=/home/node",
            "-e", f"DASHSCOPE_API_KEY={dashscope_key}",
            "-e", f"QWEN_API_KEY={qwen_key}",
            "-e", f"ANTHROPIC_API_KEY={anthropic_key}",
            "-e", f"ANTHROPIC_BASE_URL={anthropic_base_url}",
            "-e", f"ANTHROPIC_AUTH_TOKEN={anthropic_auth_token}",
            "-e", f"ANTHROPIC_MODEL={anthropic_model}",
            "-e", "TERM=xterm",
            "-e", "COLUMNS=80",
            "-w", container_workspace,
            "clawdbot",
            "bash", "-c", inner_cmd
        ]

        # 4. 执行命令 (流式执行)
        log_path = os.path.expanduser(log_path)
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        try:
            with open(log_path, "w") as log_file:
                log_file.write(f"=== ZHIWEI AUDIT START ===\n")
                log_file.write(f"TIME: {Path('/etc/timezone').read_text().strip() if Path('/etc/timezone').exists() else 'N/A'}\n")
                log_file.write(f"CMD: {' '.join(cmd)}\n")
                log_file.write(f"==========================\n\n")
                log_file.flush()

                # 使用 Popen 开启流式输出
                process = subprocess.Popen(
                    cmd,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1, # Line buffered
                    preexec_fn=os.setsid if os.name != 'nt' else None # Create process group
                )

                import time
                start_time = time.time()
                last_heartbeat = start_time
                returncode = None

                while returncode is None:
                    # 检查是否超时
                    elapsed = time.time() - start_time
                    if elapsed > self.timeout:
                        import signal
                        if os.name != 'nt':
                            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                        else:
                            process.kill()
                            
                        log_file.write(f"\n[ERROR] Task timeout after {self.timeout}s\n")
                        return ExecuteResult(
                            success=False, stdout="", 
                            stderr=f"Timeout after {self.timeout}s", 
                            returncode=-1
                        )

                    # 每 60s 发送一条进度心跳到 MessageBus (v57.0)
                    if time.time() - last_heartbeat > 60:
                        mins = int(elapsed // 60)
                        msg = f"🤖 知微大脑正在深度思考与编码中... (已耗时 {mins}min)"
                        if self.msg_bus:
                            self.msg_bus.publish(
                                sender="zhiwei-dev/backend",
                                topic="feishu_notification",
                                content=msg,
                                metadata={"type": "progress_heartbeat", "elapsed": elapsed}
                            )
                        last_heartbeat = time.time()

                    # 非阻塞等待
                    try:
                        returncode = process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        continue

            # 读取产生的日志用于返回结果（即便已经是流式写入，ExecuteResult 仍需返回内容摘要）
            with open(log_path, "r") as f:
                full_log = f.read()

            return ExecuteResult(
                success=returncode == 0,
                stdout=full_log,
                stderr="",
                returncode=returncode
            )

        except Exception as e:
            # 记录异常到日志
            with open(log_path, "a") as f:
                f.write(f"\n[CRITICAL ERROR] {str(e)}\n")
            return ExecuteResult(success=False, stdout="", stderr=str(e), returncode=-1)


# 测试代码
if __name__ == "__main__":
    # 模拟 Host 路径
    backend = ClaudeCodeBackend()
    test_dir = "/Users/liufang/clawdbot-docker/workspace/tasks/test-run"
    os.makedirs(test_dir, exist_ok=True)
    
    print(f"Testing container backend in: {test_dir}")
    result = backend.execute(
        task="Say hello and create a file named 'hello.txt' with content 'brain ok'",
        workspace=test_dir,
        log_path=f"{test_dir}/run.log"
    )
    print(f"Success: {result.success}")
    print(f"Stdout: {result.stdout[:200]}")