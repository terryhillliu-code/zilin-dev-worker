"""
开发任务 Worker
常驻进程，轮询任务队列，执行开发任务
"""

import os
import sys
import time
import json
import subprocess
import signal
import threading
from pathlib import Path
from datetime import datetime
import re

def generate_diagnosis(task_id: int, artifacts_dir: Path, error_msg: str):
    """生成失败诊断报告 (v5.6.6)"""
    diag_file = artifacts_dir / "diagnosis.md"
    log_path = artifacts_dir / "run.log"
    
    last_lines = ""
    if log_path.exists():
        try:
            with open(log_path, "r") as f:
                lines = f.readlines()
                last_lines = "".join(lines[-30:]) # Capture last 30 lines
        except: pass

    with open(diag_file, "w") as f:
        f.write(f"# 任务 #{task_id} 失败诊断报告\n\n")
        f.write(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("## 错误摘要\n")
        f.write(f"```text\n{error_msg[:1000]}\n```\n\n")
        if last_lines:
            f.write("## 容器日志末尾 (现场还原)\n")
            f.write(f"```text\n{last_lines}\n```\n\n")
        f.write("## 建议对策\n")
        if "401" in error_msg or "api_key" in error_msg.lower():
            f.write("- 检查 API 密钥对齐情况：当前使用的是百炼 SK-SP 模式。\n")
        elif "timeout" in error_msg.lower():
            f.write("- 任务执行超时，可能存在无限循环或大批量文件卡死。\n")
        else:
            f.write("- 请运行 `zw-log {task_id}` 查看完整执行轨迹。\n")

from queue import Empty
import shutil
from concurrent.futures import ThreadPoolExecutor

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

# 加载全局密钥
from zhiwei_common.secrets import load_secrets
load_secrets(silent=True)

from zhiwei_common import TaskStore
from backends.claude_code import ClaudeCodeBackend
from message_bus import MessageBus
from knowledge_client import KnowledgeClient
from verify_evidence import run_verification_for_worker, determine_evidence_level


# 配置
POLL_INTERVAL = 5  # 秒
WORKTREE_BASE = "/Users/liufang/clawdbot-docker/workspace/tasks"
ARTIFACTS_BASE = Path("/Users/liufang/zhiwei-dev/artifacts")
BASE_REPO = os.path.expanduser("~/zhiwei-scheduler")
ANTHROPIC_AUTH_TOKEN = os.environ.get("ANTHROPIC_AUTH_TOKEN", "")


# 受保护文件
PROTECTED_FILES = [
    "ws_client.py", "docker-compose.yml", ".env",
    "openclaw.json", "tasks.db"
]


HEARTBEAT_FILE = Path("/tmp/zhiwei-dev-worker.heartbeat")
HEARTBEAT_INTERVAL = 30  # 秒

# v58.0: 阶段超时阈值 (秒) - 超过则推送卡住告警
STUCK_THRESHOLDS = {
    "🔍 API 预检中...": 20,
    "准备 Artifacts": 30,
    "准备工作区": 60,
    "准备研究沙盒": 30,
    "🔍 三路召回检索知识库...": 30,
    "🤖 AI 执行中...": 300,  # 5分钟
    "🔄 瞬时异常重试中...": 60,
    "🔍 基础验证中...": 120,
    "📋 证据验证中...": 60,
    "提交代码变更": 30,
}

class Worker:
    def __init__(self, check_interval: int = 5, max_workers: int = 5):
        self.store = TaskStore()
        # v5.6: 统一为全能知微大脑，不再分流研究后端
        self.backend = ClaudeCodeBackend()
        self.check_interval = check_interval
        self.msg_bus = MessageBus()
        self.knowledge = KnowledgeClient()
        self.base_path = Path(__file__).parent.resolve()
        self._running = False
        
        # Concurrency
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_futures = set()
        self._active_tasks = set()
        self._lock = threading.Lock()
        self._git_lock = threading.Lock()

        # v58.0: 阶段计时器 (用于卡住检测)
        self._task_stage_times: dict[int, tuple[str, float]] = {}  # task_id -> (stage, start_time)

        # 信号处理 (如果是作为子线程被实例化，例如在 ws_client 中被调用评估风险，则忽略信号注册错误)
        try:
            import signal as _signal
            _signal.signal(_signal.SIGTERM, self._handle_signal)
            _signal.signal(_signal.SIGINT, self._handle_signal)
        except (ValueError, AttributeError):
            pass

    def _handle_signal(self, signum, frame):
        print(f"\n收到信号 {signum}，正在退出...")
        self._running = False

    def _log(self, msg: str):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def _check_protected(self, task_input: str) -> str | None:
        """检查是否涉及受保护文件，返回错误信息或 None"""
        task_lower = task_input.lower()
        for f in PROTECTED_FILES:
            if f.lower() in task_lower:
                return f"任务涉及受保护文件 {f}，已拒绝执行"
        return None

    def _assess_risk(self, task_input: str) -> str:
        """评估任务风险等级: auto / notify / approve

        auto:    低风险，直接执行，无需审批
        notify:  中风险，执行后推送通知
        approve: 高风险，需人工审批（当前未实现审批阻塞，等同 notify）
        """
        task_lower = task_input.lower()

        # Level 2 (approve): 涉及 CRITICAL 文件
        for f in PROTECTED_FILES:
            if f.lower() in task_lower:
                return "approve"

        # Level 2: 明确的删除/配置/部署操作
        danger_keywords = ["删除", "delete", "remove", "drop", "docker",
                           ".env", "deploy", "部署", "migration", "迁移"]
        if any(kw in task_lower for kw in danger_keywords):
            return "approve"

        # Level 1 (notify): 涉及多个目录的修改
        multi_dir_keywords = ["所有", "全部", "批量", "重构"]
        if any(kw in task_lower for kw in multi_dir_keywords):
            return "notify"

        # Level 0 (auto): 其余情况
        return "auto"

    def _setup_worktree(self, task_id: int, repo_path: str = None) -> tuple[str, str]:
        """创建 git worktree，返回 (workspace_path, branch_name)
        内置 Git Lock 并发防护：遇到锁冲突时自动重试 (指数退避)
        v58.0: 增强清理逻辑，处理残留目录
        """
        target_repo = os.path.expanduser(repo_path) if repo_path else BASE_REPO
        branch = f"dev/task-{task_id}"
        workspace = f"{WORKTREE_BASE}/task-{task_id}"

        max_retries = 3
        for attempt in range(max_retries + 1):
            with self._git_lock:
                # v58.0: 增强清理 - 先尝试 git worktree remove，再物理删除目录
                # 1. 尝试 git worktree remove
                subprocess.run(
                    ["git", "worktree", "remove", workspace, "--force"],
                    cwd=target_repo, capture_output=True
                )
                # 2. 尝试清理 worktree 记录
                subprocess.run(
                    ["git", "worktree", "prune"],
                    cwd=target_repo, capture_output=True
                )
                # 3. 删除残留分支
                subprocess.run(
                    ["git", "branch", "-D", branch],
                    cwd=target_repo, capture_output=True
                )
                # 4. 如果物理目录仍存在，强制删除
                if os.path.exists(workspace):
                    try:
                        shutil.rmtree(workspace)
                        self._log(f"  🧹 已清理残留目录: {workspace}")
                    except Exception as e:
                        self._log(f"  ⚠️ 清理目录失败: {e}")

                # 创建新 worktree
                os.makedirs(WORKTREE_BASE, exist_ok=True)
                result = subprocess.run(
                    ["git", "worktree", "add", workspace, "-b", branch],
                    cwd=target_repo, capture_output=True, text=True
                )

            if result.returncode == 0:
                return workspace, branch

            # Git lock 错误检测与重试
            stderr = result.stderr.lower()
            if ("lock" in stderr or "index.lock" in stderr) and attempt < max_retries:
                wait = 2 ** (attempt + 1)  # 2s, 4s, 8s
                self._log(f"  ⚠️ Git lock 冲突 @ {target_repo} (尝试 {attempt+1}/{max_retries})，{wait}s 后重试...")
                time.sleep(wait)
                # 清理可能残留的 lock 文件
                lock_file = os.path.join(target_repo, ".git", "index.lock")
                if os.path.exists(lock_file):
                    try:
                        os.remove(lock_file)
                        self._log(f"  🔧 已清理残留 index.lock")
                    except OSError:
                        pass
                continue

            raise RuntimeError(f"在 {target_repo} 创建 worktree 失败: {result.stderr}")

        raise RuntimeError(f"创建 worktree 失败: 重试 {max_retries} 次后仍不成功")

    def _cleanup_worktree(self, workspace: str, branch: str, keep_branch: bool = True):
        """清理 worktree"""
        with self._git_lock:
            subprocess.run(
                ["git", "worktree", "remove", workspace, "--force"],
                cwd=BASE_REPO, capture_output=True
            )
            if not keep_branch:
                subprocess.run(
                    ["git", "branch", "-D", branch],
                    cwd=BASE_REPO, capture_output=True
                )

    def _setup_artifacts(self, task_id: int) -> Path:
        """创建 artifacts 目录"""
        artifacts_dir = ARTIFACTS_BASE / str(task_id)
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        return artifacts_dir

    def _run_verify(self, workspace: str, backend_type: str = "claude") -> tuple[bool, str]:
        """代码级验证 (v57.0: 替换 pre_check_v2.sh)"""
        if backend_type == "research":
            return True, "研究任务，跳过代码验证"

        # 1. 检查是否有文件变更
        status = subprocess.run(["git", "status", "--porcelain"],
            cwd=workspace, capture_output=True, text=True)
        if not status.stdout.strip():
            return False, "❌ 没有发现文件变更，请确认是否正确执行了修改"

        # 2. 对 .py 文件做语法检查 (如果存在)
        py_files = []
        for line in status.stdout.strip().split('\n'):
            # git status --porcelain 输出格式为 " M path/to/file"
            filepath = line.strip().split()[-1]
            if filepath.endswith('.py'):
                py_files.append(filepath)

        if py_files:
            self._log(f"  🔍 对 {len(py_files)} 个 Python 文件进行语法检查...")
            for fp in py_files:
                abs_path = os.path.join(workspace, fp)
                if not os.path.exists(abs_path): continue
                
                res = subprocess.run([sys.executable, "-m", "py_compile", fp],
                    cwd=workspace, capture_output=True, text=True)
                if res.returncode != 0:
                    return False, f"❌ Python 语法错误: {fp}\n{res.stderr}"

        return True, "✅ 代码基础验证通过 (变更检测+语法检查)"

    def _run_evidence_verify(self, task_id: int, workspace: str,
                             task_input: str, artifacts_dir: Path) -> tuple[bool, str]:
        """执行证据验证 (v47.7: 使用 verify_evidence.py 三级验证)"""
        import subprocess

        # 获取变更文件列表
        status = subprocess.run(["git", "status", "--porcelain"],
            cwd=workspace, capture_output=True, text=True)
        changed_files = [line.strip() for line in status.stdout.strip().split('\n') if line.strip()]

        if not changed_files:
            return False, "❌ 没有文件变更"

        # 获取最近的提交信息
        commit_result = subprocess.run(
            ["git", "log", "-1", "--format=%s%n%b"],
            cwd=workspace, capture_output=True, text=True
        )
        commit_output = commit_result.stdout

        # v5.9: test_output 默认为空（暂无测试执行）
        test_output = ""

        # 调用三级验证
        return run_verification_for_worker(
            task_id=task_id,
            workspace=workspace,
            task_input=task_input,
            commit_output=commit_output,
            changed_files=changed_files,
            test_output=test_output
        )

    def _run_research_verify(self, task_id: int, artifacts_dir: Path) -> tuple[bool, str]:
        """验证研究任务 (只需检查报告是否存在)"""
        # 统计 artifacts 目录下的 md 文件
        md_files = list(artifacts_dir.glob("*.md"))
        if md_files:
            return True, f"✅ 已生成 {len(md_files)} 份研究报告: {', '.join(f.name for f in md_files)}"
        
        # 兜底检查 /tmp/notebooklm_export
        export_root = Path("/tmp/notebooklm_export")
        if export_root.exists():
            tmp_mds = list(export_root.glob("**/*.md"))
            if tmp_mds:
                # 尝试拷贝到 artifacts
                for f in tmp_mds:
                    try: shutil.copy(f, artifacts_dir / f.name)
                    except: pass
                return True, f"✅ 已从临时目录同步 {len(tmp_mds)} 份报告"

        return False, "❌ 未发现生成的 Markdown 研究报告"

    def _commit_changes(self, workspace: str, task_input: str) -> str | None:
        """提交变更，返回 commit sha"""
        # 检查是否有变更
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=workspace, capture_output=True, text=True
        )

        if not status.stdout.strip():
            return None  # 没有变更

        # 添加并提交
        subprocess.run(["git", "add", "-A"], cwd=workspace)

        commit_msg = f"[zhiwei-dev] {task_input[:50]}"
        subprocess.run(
            ["git", "commit", "-m", commit_msg],
            cwd=workspace, capture_output=True
        )

        # 获取 commit sha
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=workspace, capture_output=True, text=True
        )

        return result.stdout.strip()

    def _get_diff_stat(self, workspace: str) -> str:
        """获取 diff 统计"""
        result = subprocess.run(
            ["git", "diff", "main", "--stat"],
            cwd=workspace, capture_output=True, text=True
        )
        return result.stdout

    def _auto_merge(self, workspace: str, branch: str, task_id: int):
        """自动合并变更到主分支 (v57.0)"""
        self._log(f"  🚀 开启自动合并模式 (Task #{task_id})")
        
        # 1. 切换回 main
        subprocess.run(["git", "checkout", "main"], cwd=workspace, capture_output=True)
        
        # 2. 合并分支
        merge_res = subprocess.run(["git", "merge", branch, "--no-edit"], 
                                   cwd=workspace, capture_output=True, text=True)
        
        if merge_res.returncode != 0:
            self._log(f"  ❌ 自动合并冲突: {merge_res.stderr}")
            raise RuntimeError(f"自动合并冲突，请手动处理: {merge_res.stderr}")
            
        self._log(f"  ✅ 自动合并成功")

    def _push_feishu(self, task_id: int, success: bool, message: str):
        """推送结果到统一消息总线(Message Bus)"""
        # 获取当天序号
        daily_seq = self.store.get_daily_seq(task_id)

        emoji = "✅" if success else "❌"
        status = "完成" if success else "失败"
        content = f"{emoji} 开发任务 今日#{daily_seq} {status}\n\n{message}"

        # v32.4: 从 task metadata 获取 user_id，不再依赖物理文件
        task_record = self.store.get(task_id)
        user_id = None
        if task_record and task_record.get("message_id"):
            # 如果 message_id 包含路由信息或在 db 中有记录，此处提取
            # 目前逻辑：通过 user_mappings 目录兼容旧逻辑，但优先从 metadata 提取
            user_mappings_dir = self.base_path / "user_mappings"
            user_file = user_mappings_dir / f"task_{task_id}_user.json"
            if user_file.exists():
                try:
                    with open(user_file, 'r') as f:
                        user_id = json.load(f).get("user_id")
                except: pass

        metadata = {
            "task_id": task_id,
            "success": success,
            "title": f"知微开发任务 #{task_id}",
            "targets": ["feishu"],
            "refine": True  # 启用 Agent 润色
        }
        if user_id:
            metadata["user_id"] = user_id

        # 统一使用 feishu_notification 主题，直接对接机器人消费端
        self.msg_bus.publish(
            sender="zhiwei-dev/worker",
            topic="feishu_notification", 
            content=content,
            metadata=metadata
        )

        self._log(f"通知已发布至 MessageBus: 任务 今日#{daily_seq}, 成功={success}")

    def _push_feishu_progress(self, task_id: int, stage: str, elapsed: float = None):
        """v58.0: 推送进度到飞书 (轻量版，不等待确认)"""
        daily_seq = self.store.get_daily_seq(task_id)
        task_input = self.store.get(task_id).get("input", "")[:40] if self.store.get(task_id) else ""

        elapsed_str = f" (已 {int(elapsed)}s)" if elapsed else ""
        content = f"📊 任务 今日#{daily_seq}: {stage}{elapsed_str}\n> {task_input}..."

        task_record = self.store.get(task_id)
        user_id = None
        if task_record and task_record.get("message_id"):
            user_mappings_dir = self.base_path / "user_mappings"
            user_file = user_mappings_dir / f"task_{task_id}_user.json"
            if user_file.exists():
                try:
                    with open(user_file, 'r') as f:
                        user_id = json.load(f).get("user_id")
                except: pass

        metadata = {
            "task_id": task_id,
            "stage": stage,
            "elapsed": elapsed,
            "type": "progress_update",
            "targets": ["feishu"],
            "refine": False  # 进度消息不润色，保持简洁
        }
        if user_id:
            metadata["user_id"] = user_id

        # 非阻塞发布
        try:
            self.msg_bus.publish(
                sender="zhiwei-dev/worker",
                topic="feishu_notification",
                content=content,
                metadata=metadata
            )
        except Exception as e:
            self._log(f"  ⚠️ 进度推送失败: {e}")

    def _update_stage_with_alert(self, task_id: int, stage: str):
        """v58.0: 更新阶段 + 推送飞书 + 记录时间 (用于卡住检测)"""
        self.store.update_progress(task_id, stage)
        self._task_stage_times[task_id] = (stage, time.time())
        self._push_feishu_progress(task_id, stage)

    def _check_api_available(self) -> tuple[bool, str]:
        """v58.1: 检查 API 是否可用 (容器内预检)
        返回 (is_available, error_message)
        """
        # 获取环境变量
        anthropic_auth_token = os.environ.get("ANTHROPIC_AUTH_TOKEN", "")
        anthropic_base_url = os.environ.get("ANTHROPIC_BASE_URL", "")

        # 检查必要的环境变量
        if not anthropic_auth_token:
            return False, "ANTHROPIC_AUTH_TOKEN 未设置"

        # 在容器内执行一个简单的测试命令
        test_cmd = [
            "docker", "exec",
            "--user", "1000:1000",
            "-e", "HOME=/home/node",
            "-e", f"ANTHROPIC_AUTH_TOKEN={anthropic_auth_token}",
            "-e", f"ANTHROPIC_BASE_URL={anthropic_base_url}",
            "-e", "ANTHROPIC_MODEL=glm-5",
            "clawdbot",
            "bash", "-c",
            "echo 'test' | timeout 10 npx -y @anthropic-ai/claude-code --print 2>&1 | head -5"
        ]

        try:
            result = subprocess.run(
                test_cmd,
                capture_output=True,
                text=True,
                timeout=15
            )

            # 检查是否有认证错误
            output = result.stdout + result.stderr
            if "403" in output or "forbidden" in output.lower():
                return False, "API 认证失败 (403 Forbidden)"
            if "401" in output or "unauthorized" in output.lower():
                return False, "API 认证失败 (401 Unauthorized)"
            if "api_key" in output.lower() and "error" in output.lower():
                return False, "API Key 无效"

            return True, "OK"

        except subprocess.TimeoutExpired:
            return False, "API 预检超时 (15s)"
        except Exception as e:
            return False, f"预检异常: {str(e)}"

    def _check_and_alert_stuck(self, task_id: int):
        """v58.0: 检查是否卡住，推送告警"""
        if task_id not in self._task_stage_times:
            return

        stage, start_time = self._task_stage_times[task_id]
        elapsed = time.time() - start_time
        threshold = STUCK_THRESHOLDS.get(stage, 120)  # 默认 2 分钟

        if elapsed > threshold:
            # 推送卡住告警
            self._push_feishu_progress(
                task_id,
                f"⚠️ 可能卡住: {stage}",
                elapsed=elapsed
            )
            self._log(f"  ⚠️ 任务 #{task_id} 在 '{stage}' 阶段停留 {int(elapsed)}s > 阈值 {threshold}s")

    def execute_task(self, task: dict):
        """执行单个任务"""
        task_id = task["id"]
        task_input = task["input"]
        backend_type = task.get("backend", "claude")

        self._log(f"开始执行任务 #{task_id} ({backend_type}): {task_input[:50]}...")

        # v58.0: 任务开始推送
        self._push_feishu_progress(task_id, "🚀 任务开始执行")

        # 检查受保护文件
        self._update_stage_with_alert(task_id, "检查安全规则")
        protected_error = self._check_protected(task_input)
        if protected_error:
            self.store.fail(task_id, protected_error)
            self._push_feishu(task_id, False, protected_error)
            return

        # v32.4: 风险评估
        risk_level = self._assess_risk(task_input)
        self._log(f"  风险等级: {risk_level}")

        # v58.1: API 预检 (仅 claude 后端)
        if backend_type == "claude":
            self._update_stage_with_alert(task_id, "🔍 API 预检中...")
            api_ok, api_error = self._check_api_available()
            if not api_ok:
                self._log(f"  ❌ API 预检失败: {api_error}")
                self.store.fail(task_id, f"API 预检失败: {api_error}")
                self._push_feishu(task_id, False, f"🚨 API 预检失败\n\n**错误**: {api_error}\n\n请检查 API Key 配置或联系管理员。")
                return
            self._log(f"  ✅ API 预检通过")

        workspace = None
        branch = None
        artifacts_dir = None

        try:
            # 1. 创建 artifacts 目录
            self._update_stage_with_alert(task_id, "准备 Artifacts")
            artifacts_dir = self._setup_artifacts(task_id)

            with open(artifacts_dir / "input.json", "w") as f:
                json.dump({"id": task_id, "input": task_input}, f, ensure_ascii=False, indent=2)

            # 2. 准备工作区
            if backend_type == "claude":
                repo_path = task.get("repo_path")
                workspace, branch = self._setup_worktree(task_id, repo_path=repo_path)
                self._update_stage_with_alert(task_id, f"准备工作区 ({os.path.basename(repo_path or 'default')})")
                self._log(f"  Worktree: {workspace} (Repo: {repo_path or 'BASE'})")
            else:
                # 研究任务，直接在 artifacts 目录下执行，不建立 Git 关联 (v57.0)
                workspace = str(artifacts_dir)
                branch = None
                self._update_stage_with_alert(task_id, "准备研究沙盒")
                self._log(f"  Research Worktree: {workspace}")

            # 2.5 深度知识库集成 (RAG v2)
            context = ""
            if self.knowledge.should_trigger_rag(task_input):
                self._update_stage_with_alert(task_id, "🔍 三路召回检索知识库...")
                try:
                    # P1 优化：增加 RAG 检索超时保护 (利用 ThreadPoolExecutor 或简单 timer)
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as rag_executor:
                        future = rag_executor.submit(self.knowledge.get_context, task_input, top_k=3)
                        context = future.result(timeout=5)  # 5秒强制超时
                    
                    if context:
                        self._log(f"  RAG 命中: 已注入参考内容 ({len(context)} 字符)")
                    else:
                        self._log(f"  RAG: 未命中相关知识")
                except concurrent.futures.TimeoutError:
                    self._log(f"  ⚠️ RAG 检索超时 (5s)，降级跳过检索")
                except Exception as e:
                    self._log(f"  RAG 检索失败 (降级继续): {e}")

            # 3. 执行后端 (同模型智能重试)
            self._update_stage_with_alert(task_id, "🤖 AI 执行中...")
            log_path = str(artifacts_dir / "run.log")
            task_model = task.get("model")
            self._log(f"  Target Model: {task_model or 'Default'}")
            
            # v5.6: 始终使用统一的知微大脑 (Holistic Agent)
            active_backend = self.backend
            
            # [Phase 6] Token 监控埋点
            final_input = f"{context}\n\n{task_input}" if context else task_input
            token_stats = {
                "timestamp": datetime.now().isoformat(),
                "task_id": task_id,
                "model": task_model or "default",
                "input_chars": len(final_input),
                "context_chars": len(context) if context else 0,
                "orchestrator_directive_chars": len(task_input)
            }
            with open(artifacts_dir / "token_usage.json", "w") as f:
                json.dump(token_stats, f, indent=2)
            self._log(f"  Token Stats Recorded: {len(final_input)} chars")

            # v56.0: 读取验证阶段沉淀的错误上下文 (用于自愈)
            retry_context = task.get("verify_result")
            self._log(f"  Retry Context: {'Present' if retry_context else 'None'}")

            result = active_backend.execute(
                final_input, workspace, log_path, 
                model_override=task_model,
                retry_context=retry_context,
                bypass_prompts=(risk_level == "auto")
            )

            # 首次失败时：同模型重试 (针对 API/网络等瞬时错误)
            if not result.success and not retry_context:
                self._log(f"  首次执行异常: {result.stderr[:80]}...")
                self._update_stage_with_alert(task_id, "🔄 瞬时异常重试中...")

                shutil.copy(log_path, str(artifacts_dir / "run_attempt1.log"))
                retry_log = str(artifacts_dir / "run_retry.log")

                result = self.backend.execute(
                    final_input, workspace, retry_log, 
                    model_override=task_model,
                    bypass_prompts=(risk_level == "auto")
                )

                if result.success:
                    shutil.copy(retry_log, log_path)
                    self._log(f"  重试成功!")

            # 4. 基础验证 (v34.0: 引入状态流)
            self._update_stage_with_alert(task_id, "🔍 基础验证中...")
            self.store.start_verify(task_id)

            verify_ok, verify_output = self._run_verify(workspace, backend_type=backend_type)
            with open(artifacts_dir / "verify_basic.log", "w") as f:
                f.write(verify_output)

            if not verify_ok:
                updated, can_retry = self.store.verify_fail(task_id, f"基础验证失败: {verify_output[:200]}")
                if can_retry:
                    self._push_feishu(task_id, False, f"⚠️ 验证失败，自动重试中...\n\n{verify_output[:500]}")
                    return  # 保留 worktree，等待重新执行
                else:
                    self._push_feishu(task_id, False, f"❌ 验证失败（已达重试上限）\n\n{verify_output[:500]}")
                    return

            # 5. 证据验证 (分流)
            if backend_type == "research":
                self._update_stage_with_alert(task_id, "📋 研究成果验证中...")
                evidence_ok, evidence_report = self._run_research_verify(task_id, artifacts_dir)
            else:
                self._update_stage_with_alert(task_id, "📋 证据验证中...")
                evidence_ok, evidence_report = self._run_evidence_verify(task_id, workspace, task_input, artifacts_dir)

            if not evidence_ok:
                updated, can_retry = self.store.verify_fail(task_id, f"验证失败: {evidence_report[:200]}")
                if can_retry:
                    self._push_feishu(task_id, False, f"⚠️ 验证失败，自动重试中...\n\n{evidence_report[:500]}")
                    return
                else:
                    self._push_feishu(task_id, False, f"❌ 验证失败（已达重试上限）\n\n{evidence_report[:500]}")
                    return

            with open(artifacts_dir / "verify_evidence.log", "w") as f:
                f.write(evidence_report)

            # 6. 提交变更 (分流)
            commit_sha = "N/A"
            diff_stat = "N/A"

            if backend_type == "claude":
                self._update_stage_with_alert(task_id, "提交代码变更")
                commit_sha = self._commit_changes(workspace, task_input)

                if not commit_sha:
                    self._update_stage_with_alert(task_id, "⚠️ 无文件变更")
                    self.store.fail(task_id, "没有文件变更")
                    self._push_feishu(task_id, False, "执行完成但没有文件变更")
                    return

                # 7. 生成 Diff
                self._update_stage_with_alert(task_id, "生成 Diff 报告")
                diff_stat = self._get_diff_stat(workspace)
                with open(artifacts_dir / "diff.patch", "w") as f:
                    f.write(diff_stat)
            else:
                self._update_stage_with_alert(task_id, "✅ 研究成果已保存")
                diff_stat = evidence_report

            # 8. 保存元信息
            with open(artifacts_dir / "meta.json", "w") as f:
                json.dump({
                    "task_id": task_id,
                    "branch": branch or "N/A",
                    "commit_sha": commit_sha,
                    "backend": backend_type,
                    "success": True,
                    "finished_at": datetime.now().isoformat()
                }, f, indent=2)

            # 9. 任务完结逻辑 (v57.0: 区分自动合并与人工确认)
            if risk_level == "auto" and backend_type == "claude":
                self._update_stage_with_alert(task_id, "🚀 自动合并中")
                try:
                    self._auto_merge(workspace, branch, task_id)
                    self.store.complete(task_id, commit_sha=commit_sha, result=evidence_report)

                    self._push_feishu(task_id, True, f"✅ 已自动完成并合并至主分支\n\n**变更统计**:\n{diff_stat}\n\n**验证证据**:\n{evidence_report[:400]}")
                    self._log(f"  任务 #{task_id} 自动完成")
                    return # 正常清理
                except Exception as e:
                    self._log(f"  ⚠️ 自动合并失败，回退到人工确认: {e}")
                    # Fall through to await_review

            self._update_stage_with_alert(task_id, "⏳ 等待人工确认")
            self.store.await_review(task_id, evidence_report, task_input[:100])

            message = f"""📋 验证通过，请确认后合并

**任务**: {task_input[:50]}...
**分支**: {branch}
**Commit**: {commit_sha[:8]}

**验证证据**:
{evidence_report[:800]}

{diff_stat}

---
✅ 回复「确认」或 `/accept {task_id}`
❌ 回复「重做 原因」或 `/reject {task_id} 原因`"""

            self._push_feishu(task_id, True, message)
            self._log(f"  任务 #{task_id} 等待人工确认")
            return  # 不清理 worktree

        except Exception as e:
            import traceback
            error_msg = traceback.format_exc()
            self._log(f"  任务 #{task_id} 异常: {error_msg}")
            self._update_stage_with_alert(task_id, f"❌ 异常: {str(e)[:30]}")
            self.store.fail(task_id, error_msg)
            
            # v5.6.9: 立即触发飞书告警并诊断
            self._push_feishu(task_id, False, f"🚨 任务失败告警!\n错误: {str(e)}\n请在终端执行 zw-log {task_id} 查看现场。")
            
            if artifacts_dir:
                generate_diagnosis(task_id, artifacts_dir, error_msg)

        finally:
            # v58.0: 清理阶段计时器
            self._task_stage_times.pop(task_id, None)

            # v34.0: 只有 done/failed 才清理 worktree，awaiting_review 保留
            task_record = self.store.get(task_id)
            should_cleanup = task_record and task_record.get("status") in ["failed", "done"]
            if workspace and branch and should_cleanup:
                self._cleanup_worktree(workspace, branch, keep_branch=True)

            # v32.5: 如果任务失败，触发 CriticAgent 异步复盘
            task_record = self.store.get(task_id)
            if task_record and task_record.get("status") == "failed":
                critic_script = self.base_path / "critic_agent.py"
                if critic_script.exists():
                    self._log(f"  📢 任务 #{task_id} 失败，正在启动 CriticAgent 异步复盘...")
                    try:
                        # 异步执行，不阻塞 Worker 主任务循环
                        subprocess.Popen(
                            [sys.executable, str(critic_script), str(task_id)],
                            cwd=str(self.base_path),
                            env={**os.environ, "ANTHROPIC_AUTH_TOKEN": ANTHROPIC_AUTH_TOKEN}
                        )
                    except Exception as e:
                        self._log(f"  ⚠️ 触发 CriticAgent 失败: {e}")
                else:
                    self._log(f"  ⏭️ CriticAgent 未安装，跳过复盘")

    def _heartbeat_loop(self):
        """v32.6: 心跳线程，优化空闲时的写入频率"""
        last_data = None
        while self._running:
            try:
                with self._lock:
                    active_tasks = list(self._active_tasks)

                status = "executing" if active_tasks else "idle"
                data = {
                    "pid": os.getpid(),
                    "active_tasks": active_tasks,
                    "status": status,
                    "pool_size": len(self.active_futures)
                }

                # 如果状态没变且处于 idle，则延长写入间隔 (P1 优化)
                if data == last_data and status == "idle":
                    pass
                else:
                    full_data = {**data, "timestamp": time.time(), "iso": time.strftime("%Y-%m-%d %H:%M:%S")}
                    HEARTBEAT_FILE.write_text(json.dumps(full_data))
                    last_data = data

                # v58.0: 检查是否卡住 (每 30 秒检查一次)
                for task_id in active_tasks:
                    self._check_and_alert_stuck(task_id)

            except Exception:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def run(self):
        """主循环"""
        self._running = True
        self._log(f"Worker 启动，最大并发数: {self.max_workers}")

        # v32.4: 启动心跳线程
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        self._log("❤️ 心跳线程已启动")

        # 恢复超时任务
        self.store.recover_stale(timeout_minutes=10)

        while self._running:
            # 清理已完成的 future
            self.active_futures = {f for f in self.active_futures if not f.done()}

            if len(self.active_futures) >= self.max_workers:
                time.sleep(POLL_INTERVAL)
                continue

            # v5.6: 统一任务认领逻辑
            task = self.store.claim_next()
            
            if task:
                self._log(f"  认领任务 #{task['id']} (Backend: {task['backend']})")
                future = self.executor.submit(self._execute_task_wrapped, task)
                self.active_futures.add(future)
            else:
                # v5.6.10 Debug: 每 10 次空循环记录一次
                if not hasattr(self, '_idle_count'): self._idle_count = 0
                self._idle_count += 1
                if self._idle_count >= 12: # 每分钟一次
                    self._log(f"  [Idle] 正在轮询队列... (当前最大并发: {self.max_workers}, 活动中: {len(self.active_futures)})")
                    self._idle_count = 0
                time.sleep(POLL_INTERVAL)

        self._log("Worker 退出")
        self.executor.shutdown(wait=False)
        # 清理心跳文件
        try:
            HEARTBEAT_FILE.unlink(missing_ok=True)
        except Exception:
            pass

    def _execute_task_wrapped(self, task):
        task_id = task["id"]
        with self._lock:
            self._active_tasks.add(task_id)
        try:
            self.execute_task(task)
        finally:
            with self._lock:
                self._active_tasks.discard(task_id)


if __name__ == "__main__":
    worker = Worker()
    worker.run()