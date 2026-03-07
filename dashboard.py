#!/usr/bin/env python3
"""
知微系统实时监控面板
用法: ~/zhiwei-scheduler/venv/bin/python ~/zhiwei-dev/dashboard.py
"""

import os
import sys
import time
import sqlite3
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from collections import deque

from rich.console import Console
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.live import Live
from rich.text import Text
from rich.align import Align

DB_PATH = Path(__file__).parent / "tasks.db"
DISPATCHER_LOG = Path.home() / "logs" / "dispatcher.log"
INTEGRATION_WORKTREE = Path.home() / "zhiwei-ops" / "worktrees"
LOG_FILES = {
    "Worker": Path.home() / "logs" / "dev-worker.log",
    "飞书Bot": Path.home() / "logs" / "feishu_bot.log",
    "调度器": Path.home() / "logs" / "scheduler.log",
    "调微": DISPATCHER_LOG,
}

STEP_ICONS = {
    "检查安全规则": "🔒",
    "准备 Artifacts": "📦",
    "创建隔离工作区": "🌿",
    "🔍 三路召回检索知识库...": "🧠",
    "🤖 AI 执行中...": "🤖",
    "验证代码变更": "🔍",
    "提交代码变更": "💾",
    "生成 Diff 报告": "📊",
    "✅ 完成": "✅",
}

# 步骤顺序（用于进度条）
STEP_ORDER = [
    "检查安全规则",
    "准备 Artifacts",
    "创建隔离工作区",
    "🔍 三路召回检索知识库...",
    "🤖 AI 执行中...",
    "验证代码变更",
    "提交代码变更",
    "生成 Diff 报告",
    "✅ 完成",
]


class Dashboard:
    def __init__(self):
        self.console = Console()
        self.log_buffer = deque(maxlen=15)
        self.last_log_positions = {}

    def _check_process(self, pattern: str) -> tuple[bool, str, str]:
        """检查进程是否运行，返回 (running, pid, uptime)"""
        try:
            result = subprocess.run(
                ["pgrep", "-f", pattern],
                capture_output=True, text=True
            )
            if result.stdout.strip():
                pid = result.stdout.strip().split("\n")[0]
                # 获取运行时间
                ps_result = subprocess.run(
                    ["ps", "-o", "etime=", "-p", pid],
                    capture_output=True, text=True
                )
                uptime = ps_result.stdout.strip()
                return True, pid, uptime
        except:
            pass
        return False, "", ""

    def _check_docker(self) -> tuple[bool, str]:
        """检查 Docker 容器状态"""
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", "name=clawdbot", "--format", "{{.Status}}"],
                capture_output=True, text=True
            )
            status = result.stdout.strip()
            return bool(status), status or "未运行"
        except:
            return False, "Docker 不可用"

    def build_services_panel(self) -> Panel:
        """构建服务状态面板"""
        table = Table(show_header=True, header_style="bold cyan", box=None, padding=(0, 1))
        table.add_column("服务", width=16)
        table.add_column("状态", width=6)
        table.add_column("PID", width=8)
        table.add_column("运行时间", width=12)

        services = [
            ("飞书机器人", "ws_client.py"),
            ("定时调度器", "scheduler.py"),
            ("开发 Worker", "worker.py"),
            ("调微 Dispatcher", "supervisor_daemon.py"),
        ]

        for name, pattern in services:
            running, pid, uptime = self._check_process(pattern)
            if running:
                table.add_row(name, "[green]● 运行[/]", pid, uptime)
            else:
                table.add_row(name, "[red]● 停止[/]", "-", "-")

        # Docker
        docker_ok, docker_status = self._check_docker()
        if docker_ok:
            table.add_row("OpenClaw", "[green]● 运行[/]", "-", docker_status[:12])
        else:
            table.add_row("OpenClaw", "[red]● 停止[/]", "-", docker_status[:12])

        # IntegratorAgent 状态
        int_status = self._check_integration_status()
        if int_status:
            table.add_row("集成引擎", "[green]● 就绪[/]", "-", int_status)
        else:
            table.add_row("集成引擎", "[dim]● 空闲[/]", "-", "-")

        return Panel(table, title="[bold]服务状态[/]", border_style="blue")

    def _check_integration_status(self) -> str:
        """检查 IntegratorAgent 最近的集成状态"""
        try:
            # 查看是否存在集成 worktree
            int_dirs = list(INTEGRATION_WORKTREE.glob("int_*"))
            if int_dirs:
                latest = max(int_dirs, key=lambda p: p.stat().st_mtime)
                age_min = int((time.time() - latest.stat().st_mtime) / 60)
                return f"{latest.name} ({age_min}m前)"

            # 查看 dispatcher 日志中的最后集成记录
            if DISPATCHER_LOG.exists():
                result = subprocess.run(
                    ["grep", "-c", "IntegratorAgent", str(DISPATCHER_LOG)],
                    capture_output=True, text=True
                )
                count = int(result.stdout.strip() or "0")
                if count > 0:
                    return f"历史 {count} 次"
        except Exception:
            pass
        return ""

    def _make_progress_bar(self, progress: str) -> str:
        """根据当前步骤生成进度条"""
        if not progress:
            return "[dim]等待中...[/]"

        try:
            current_idx = STEP_ORDER.index(progress)
        except ValueError:
            # 错误或自定义状态
            if "❌" in progress:
                return f"[red]{progress}[/]"
            if "⚠️" in progress:
                return f"[yellow]{progress}[/]"
            return progress

        total = len(STEP_ORDER)
        filled = current_idx + 1
        bar = "█" * filled + "░" * (total - filled)
        pct = int(filled / total * 100)
        return f"[cyan]{bar}[/] {pct}% {progress}"

    def build_tasks_panel(self) -> Panel:
        """构建任务列表面板"""
        if not DB_PATH.exists():
            return Panel("[dim]任务数据库不存在[/]", title="[bold]开发任务[/]", border_style="green")

        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.row_factory = sqlite3.Row

            # 统计
            stats = {}
            for row in conn.execute("SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status"):
                stats[row["status"]] = row["cnt"]

            total = sum(stats.values())
            summary = Text()
            summary.append(f"总计 {total}  ", style="bold")
            summary.append(f"👀{stats.get('review', 0)} ", style="magenta")
            summary.append(f"⏳{stats.get('pending', 0)} ", style="yellow")
            summary.append(f"🔄{stats.get('running', 0)} ", style="cyan")
            summary.append(f"✅{stats.get('done', 0)} ", style="green")
            summary.append(f"❌{stats.get('failed', 0)} ", style="red")
            summary.append(f"🚫{stats.get('rejected', 0)}", style="dim")

            # 任务表格
            table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
            table.add_column("#", width=4, justify="right")
            table.add_column("状态", width=6)
            table.add_column("任务", width=30)
            table.add_column("进度", width=35)
            table.add_column("耗时", width=8)

            tasks = conn.execute("""
                SELECT t.id, t.status, t.input, t.progress, t.created_at, t.started_at, t.finished_at, t.error, t.branch,
                       (SELECT COUNT(*) FROM task_dependencies d
                        JOIN tasks p ON p.id = d.depends_on_id
                        WHERE d.task_id = t.id AND p.status != 'done') as blocking_deps
                FROM tasks t ORDER BY t.created_at DESC LIMIT 8
            """).fetchall()

            for t in tasks:
                # 计算当天序号
                date_str = (t["created_at"] or "")[:10]
                today = datetime.now().strftime("%Y-%m-%d")
                if date_str == today:
                    # 今天的任务显示当天序号
                    daily_seq = conn.execute(
                        "SELECT COUNT(*) as seq FROM tasks WHERE created_at LIKE ? AND id <= ?",
                        (f"{date_str}%", t["id"])
                    ).fetchone()["seq"]
                    display_id = f"#{daily_seq}"
                else:
                    # 历史任务显示日期
                    display_id = date_str[5:] if date_str else "?"

                # 计算有效状态
                eff_status = t["status"]
                if eff_status == "pending" and t["blocking_deps"] > 0:
                    eff_status = "blocked"

                # 状态图标
                status_map = {
                    "review": "[magenta]👀审批[/]",
                    "pending": "[yellow]⏳等待[/]",
                    "blocked": "[yellow]🚧阻塞[/]",
                    "running": "[cyan]🔄执行[/]",
                    "done": "[green]✅完成[/]",
                    "failed": "[red]❌失败[/]",
                    "rejected": "[dim]🚫拒绝[/]",
                    "canceled": "[dim]🚫取消[/]",
                }
                status_str = status_map.get(eff_status, eff_status)

                # 任务描述
                task_desc = t["input"][:28]
                if len(t["input"]) > 28:
                    task_desc += ".."

                # 进度
                if t["status"] == "running":
                    progress_str = self._make_progress_bar(t["progress"] or "")
                elif t["status"] == "done":
                    branch = t["branch"] or ""
                    progress_str = f"[green]分支: {branch}[/]"
                elif t["status"] == "failed":
                    error = (t["error"] or "")[:30]
                    progress_str = f"[red]{error}[/]"
                elif t["status"] == "review":
                    progress_str = "[magenta]等待飞书审批[/]"
                elif t["status"] == "rejected":
                    progress_str = "[dim]用户已拒绝[/]"
                else:
                    progress_str = "[dim]待处理[/]"

                # 耗时
                duration = "-"
                if t["started_at"] and t["finished_at"]:
                    try:
                        start = datetime.strptime(t["started_at"], "%Y-%m-%d %H:%M:%S")
                        end = datetime.strptime(t["finished_at"], "%Y-%m-%d %H:%M:%S")
                        secs = int((end - start).total_seconds())
                        if secs >= 60:
                            duration = f"{secs // 60}m{secs % 60}s"
                        else:
                            duration = f"{secs}s"
                    except:
                        pass
                elif t["started_at"] and t["status"] == "running":
                    try:
                        start = datetime.strptime(t["started_at"], "%Y-%m-%d %H:%M:%S")
                        secs = int((datetime.now() - start).total_seconds())
                        duration = f"[cyan]{secs}s...[/]"
                    except:
                        pass

                table.add_row(display_id, status_str, task_desc, progress_str, duration)

            conn.close()

            # 组合
            content = Text()
            content.append_text(summary)

            from rich.console import Group
            return Panel(
                Group(summary, "", table),
                title="[bold]开发任务[/]",
                border_style="green"
            )

        except Exception as e:
            return Panel(f"[red]读取任务失败: {e}[/]", title="[bold]开发任务[/]", border_style="red")

    def _read_new_logs(self) -> list[str]:
        """读取各日志文件的最新内容"""
        new_lines = []

        for source, log_path in LOG_FILES.items():
            if not log_path.exists():
                continue

            try:
                file_size = log_path.stat().st_size
                last_pos = self.last_log_positions.get(source, max(0, file_size - 2000))

                if file_size > last_pos:
                    with open(log_path, "r", errors="ignore") as f:
                        f.seek(last_pos)
                        lines = f.readlines()
                        for line in lines[-5:]:
                            line = line.strip()
                            if line and "WebSocket 配置更新" not in line:
                                # 给日志加来源标记
                                new_lines.append(f"[dim][{source}][/] {line[:80]}")

                    self.last_log_positions[source] = file_size
            except:
                pass

        return new_lines

    def build_logs_panel(self) -> Panel:
        """构建日志面板"""
        new_lines = self._read_new_logs()
        for line in new_lines:
            self.log_buffer.append(line)

        if not self.log_buffer:
            content = "[dim]等待日志...[/]"
        else:
            lines = list(self.log_buffer)
            # 高亮错误
            formatted = []
            for line in lines:
                if "ERROR" in line or "Exception" in line or "❌" in line:
                    formatted.append(f"[red]{line}[/]")
                elif "WARNING" in line or "⚠️" in line:
                    formatted.append(f"[yellow]{line}[/]")
                elif "✅" in line or "成功" in line:
                    formatted.append(f"[green]{line}[/]")
                else:
                    formatted.append(line)
            content = "\n".join(formatted)

        return Panel(content, title="[bold]实时日志[/]", border_style="yellow", height=18)

    def build_layout(self) -> Layout:
        """构建整体布局"""
        layout = Layout()

        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="logs", size=20),
        )

        layout["body"].split_row(
            Layout(name="services", ratio=1),
            Layout(name="tasks", ratio=3),
        )

        # Header
        header_text = Text()
        header_text.append("  🔬 知微系统监控面板", style="bold white")
        header_text.append(f"  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", style="dim")
        header_text.append("  |  按 Ctrl+C 退出", style="dim")
        layout["header"].update(Panel(header_text, style="on dark_blue"))

        # Services
        layout["services"].update(self.build_services_panel())

        # Tasks
        layout["tasks"].update(self.build_tasks_panel())

        # Logs
        layout["logs"].update(self.build_logs_panel())

        return layout

    def run(self):
        """运行面板"""
        self.console.clear()

        try:
            with Live(self.build_layout(), console=self.console, refresh_per_second=1, screen=True) as live:
                while True:
                    time.sleep(2)
                    live.update(self.build_layout())
        except KeyboardInterrupt:
            self.console.print("\\n[dim]监控面板已关闭[/]")


if __name__ == "__main__":
    dashboard = Dashboard()
    dashboard.run()