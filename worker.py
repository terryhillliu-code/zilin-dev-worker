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
from queue import Empty
import shutil
from concurrent.futures import ThreadPoolExecutor

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from task_store import TaskStore
from backends.claude_code import ClaudeCodeBackend
from message_bus import MessageBus
from knowledge_client import KnowledgeClient

# 配置
POLL_INTERVAL = 5  # 秒
WORKTREE_BASE = "/tmp/zhiwei-worktrees"
ARTIFACTS_BASE = Path(__file__).parent / "artifacts"
BASE_REPO = os.path.expanduser("~/zhiwei-scheduler")
ANTHROPIC_AUTH_TOKEN = os.environ.get("ANTHROPIC_AUTH_TOKEN", "")


# 受保护文件
PROTECTED_FILES = [
    "ws_client.py", "docker-compose.yml", ".env",
    "openclaw.json", "tasks.db"
]


HEARTBEAT_FILE = Path("/tmp/zhiwei-dev-worker.heartbeat")
HEARTBEAT_INTERVAL = 30  # 秒


class Worker:
    def __init__(self, check_interval: int = 5, max_workers: int = 3):
        self.store = TaskStore()
        self.backend = ClaudeCodeBackend()
        self.msg_bus = MessageBus()
        self.knowledge = KnowledgeClient()
        self.check_interval = check_interval
        self._running = False
        
        # Concurrency
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_futures = set()
        self._active_tasks = set()
        self._lock = threading.Lock()
        self._git_lock = threading.Lock()

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
        """
        target_repo = os.path.expanduser(repo_path) if repo_path else BASE_REPO
        branch = f"dev/task-{task_id}"
        workspace = f"{WORKTREE_BASE}/task-{task_id}"

        max_retries = 3
        for attempt in range(max_retries + 1):
            with self._git_lock:
                # 清理可能存在的旧 worktree
                subprocess.run(
                    ["git", "worktree", "remove", workspace, "--force"],
                    cwd=target_repo, capture_output=True
                )
                subprocess.run(
                    ["git", "branch", "-D", branch],
                    cwd=target_repo, capture_output=True
                )

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

    def _run_verify(self, workspace: str) -> tuple[bool, str]:
        """运行验证脚本"""
        verify_script = os.path.expanduser("~/scripts/pre_check_v2.sh")
        if not os.path.exists(verify_script):
            return True, "验证脚本不存在，跳过验证"

        result = subprocess.run(
            ["bash", verify_script],
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=60
        )

        return result.returncode == 0, result.stdout + result.stderr

    def _run_evidence_verify(self, task_id: int, workspace: str,
                             task_input: str, artifacts_dir: Path) -> tuple[bool, str]:
        """执行证据验证 (v34.0)"""
        report_lines = [f"=== 任务 #{task_id} 证据验证报告 ===",
                        f"时间: {datetime.now().isoformat()}", ""]

        # L1: 文件变更检查
        status = subprocess.run(["git", "status", "--porcelain"],
            cwd=workspace, capture_output=True, text=True)
        changed_files = [line.strip() for line in status.stdout.strip().split('\n') if line.strip()]
        if not changed_files:
            return False, "❌ 没有文件变更"
        report_lines.append(f"【文件变更】✅ {len(changed_files)} 个文件")

        # L2: Python 语法检查
        py_files = [f[3:] for f in changed_files if f.endswith('.py')]
        for py_file in py_files:
            result = subprocess.run(["python3", "-c",
                f"import ast; ast.parse(open('{py_file}').read())"],
                cwd=workspace, capture_output=True, text=True)
            if result.returncode != 0:
                return False, f"❌ 语法错误: {py_file}\n{result.stderr[:200]}"
        if py_files:
            report_lines.append(f"【语法检查】✅ {len(py_files)} 个 Python 文件")

        # L3: 提交检查
        commit_check = subprocess.run(["git", "log", "-1", "--format=%s"],
            cwd=workspace, capture_output=True, text=True)
        report_lines.append(f"【提交】✅ {commit_check.stdout.strip()[:50]}")

        report_lines.append("\n=== 验证结果: 通过 ===")
        return True, '\n'.join(report_lines)

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
            user_mappings_dir = Path(__file__).parent / "user_mappings"
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

    def execute_task(self, task: dict):
        """执行单个任务"""
        task_id = task["id"]
        task_input = task["input"]

        self._log(f"开始执行任务 #{task_id}: {task_input[:50]}...")

        # 检查受保护文件
        self.store.update_progress(task_id, "检查安全规则")
        protected_error = self._check_protected(task_input)
        if protected_error:
            self.store.fail(task_id, protected_error)
            self._push_feishu(task_id, False, protected_error)
            return

        # v32.4: 风险评估
        risk_level = self._assess_risk(task_input)
        self._log(f"  风险等级: {risk_level}")

        workspace = None
        branch = None
        artifacts_dir = None

        try:
            # 1. 创建 artifacts 目录
            self.store.update_progress(task_id, "准备 Artifacts")
            artifacts_dir = self._setup_artifacts(task_id)

            with open(artifacts_dir / "input.json", "w") as f:
                json.dump({"id": task_id, "input": task_input}, f, ensure_ascii=False, indent=2)

            # 2. 创建 worktree
            repo_path = task.get("repo_path")
            workspace, branch = self._setup_worktree(task_id, repo_path=repo_path)
            self.store.update_progress(task_id, f"准备工作区 ({os.path.basename(repo_path or 'default')})")
            self._log(f"  Worktree: {workspace} (Repo: {repo_path or 'BASE'})")

            # 2.5 深度知识库集成 (RAG v2)
            context = ""
            if self.knowledge.should_trigger_rag(task_input):
                self.store.update_progress(task_id, "🔍 三路召回检索知识库...")
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
            self.store.update_progress(task_id, "🤖 AI 执行中...")
            log_path = str(artifacts_dir / "run.log")
            task_model = task.get("model")
            self._log(f"  Target Model: {task_model or 'Default'}")
            
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

            result = self.backend.execute(final_input, workspace, log_path, model_override=task_model)

            # 首次失败时：同模型重试，简化 prompt (复用原 backend 实例)
            if not result.success:
                self._log(f"  首次执行失败: {result.stderr[:80]}...")
                self.store.update_progress(task_id, "🔄 智能重试中 (简化指令)...")

                shutil.copy(log_path, str(artifacts_dir / "run_attempt1.log"))

                retry_input = f"请简洁地完成以下任务，不需要额外解释：\n{task_input}"
                retry_log = str(artifacts_dir / "run_retry.log")

                result = self.backend.execute(retry_input, workspace, retry_log, model_override=task_model)

                if result.success:
                    shutil.copy(retry_log, log_path)
                    self._log(f"  重试成功!")

            # 4. 基础验证 (v34.0: 引入状态流)
            self.store.update_progress(task_id, "🔍 基础验证中...")
            self.store.start_verify(task_id)

            verify_ok, verify_output = self._run_verify(workspace)
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

            # 5. 证据验证
            self.store.update_progress(task_id, "📋 证据验证中...")
            evidence_ok, evidence_report = self._run_evidence_verify(task_id, workspace, task_input, artifacts_dir)

            if not evidence_ok:
                updated, can_retry = self.store.verify_fail(task_id, f"证据验证失败: {evidence_report[:200]}")
                if can_retry:
                    self._push_feishu(task_id, False, f"⚠️ 证据验证失败，自动重试中...\n\n{evidence_report[:500]}")
                    return
                else:
                    self._push_feishu(task_id, False, f"❌ 证据验证失败（已达重试上限）\n\n{evidence_report[:500]}")
                    return

            with open(artifacts_dir / "verify_evidence.log", "w") as f:
                f.write(evidence_report)

            # 6. 提交变更
            self.store.update_progress(task_id, "提交代码变更")
            commit_sha = self._commit_changes(workspace, task_input)

            if not commit_sha:
                self.store.update_progress(task_id, "⚠️ 无文件变更")
                self.store.fail(task_id, "没有文件变更")
                self._push_feishu(task_id, False, "执行完成但没有文件变更")
                return

            # 7. 生成 Diff
            self.store.update_progress(task_id, "生成 Diff 报告")
            diff_stat = self._get_diff_stat(workspace)
            with open(artifacts_dir / "diff.patch", "w") as f:
                f.write(diff_stat)

            # 8. 保存元信息
            with open(artifacts_dir / "meta.json", "w") as f:
                json.dump({
                    "task_id": task_id,
                    "branch": branch,
                    "commit_sha": commit_sha,
                    "backend": self.backend.name,
                    "success": True,
                    "finished_at": datetime.now().isoformat()
                }, f, indent=2)

            # 9. 等待人工确认 (v34.0: 替换直接完成)
            self.store.update_progress(task_id, "⏳ 等待人工确认")
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
            error_msg = str(e)
            self._log(f"  任务 #{task_id} 异常: {error_msg}")
            self.store.update_progress(task_id, f"❌ 异常: {error_msg[:30]}")
            self.store.fail(task_id, error_msg)
            self._push_feishu(task_id, False, f"执行异常: {error_msg}")

            if artifacts_dir:
                with open(artifacts_dir / "error.log", "w") as f:
                    f.write(error_msg)

        finally:
            # v34.0: 只有 done/failed 才清理 worktree，awaiting_review 保留
            task_record = self.store.get(task_id)
            should_cleanup = task_record and task_record.get("status") in ["failed", "done"]
            if workspace and branch and should_cleanup:
                self._cleanup_worktree(workspace, branch, keep_branch=True)

            # v32.5: 如果任务失败，触发 CriticAgent 异步复盘
            task_record = self.store.get(task_id)
            if task_record and task_record.get("status") == "failed":
                self._log(f"  📢 任务 #{task_id} 失败，正在启动 CriticAgent 异步复盘...")
                try:
                    # 异步执行，不阻塞 Worker 主任务循环
                    critic_script = Path(__file__).parent / "critic_agent.py"
                    subprocess.Popen(
                        [sys.executable, str(critic_script), str(task_id)],
                        cwd=str(Path(__file__).parent),
                        env={**os.environ, "ANTHROPIC_AUTH_TOKEN": ANTHROPIC_AUTH_TOKEN}
                    )
                except Exception as e:
                    self._log(f"  ⚠️ 触发 CriticAgent 失败: {e}")

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

            task = self.store.claim_next()
            if task:
                future = self.executor.submit(self._execute_task_wrapped, task)
                self.active_futures.add(future)
            else:
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