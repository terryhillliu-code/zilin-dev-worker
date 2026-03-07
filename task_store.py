"""
SQLite 任务队列
支持：入队、认领、完成、失败、取消、列表
"""

import sqlite3
import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent / "tasks.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    status        TEXT DEFAULT 'pending',
    input         TEXT NOT NULL,
    message_id    TEXT UNIQUE,
    backend       TEXT DEFAULT 'claude',

    branch        TEXT,
    workspace     TEXT,
    commit_sha    TEXT,
    result        TEXT,
    error         TEXT,

    created_at    TEXT DEFAULT (datetime('now', 'localtime')),
    started_at    TEXT,
    finished_at   TEXT,

    attempts      INTEGER DEFAULT 0,
    max_attempts  INTEGER DEFAULT 2,
    progress      TEXT DEFAULT '',
    repo_path     TEXT,     -- v32.6: 支持多仓库协同
    model         TEXT      -- v33.0: 支持动态模型路由
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id INTEGER,
    depends_on_id INTEGER,
    PRIMARY KEY (task_id, depends_on_id),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    FOREIGN KEY (depends_on_id) REFERENCES tasks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_message_id ON tasks(message_id);
CREATE INDEX IF NOT EXISTS idx_task_deps ON task_dependencies(task_id);
"""


class TaskStore:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DB_PATH)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path, timeout=15.0) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.executescript(SCHEMA)

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        try:
            yield conn
            conn.commit()  # ✅ 正常退出时 commit
        except Exception:
            conn.rollback()  # ✅ 异常时 rollback
            raise
        finally:
            conn.close()

    def enqueue(self, task_input: str, message_id: str = None, initial_status: str = 'pending', depends_on: list[int] = None, **kwargs) -> int:
        """入队任务，返回 task_id。message_id 用于幂等去重"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            # 幂等检查
            if message_id:
                existing = conn.execute(
                    "SELECT id FROM tasks WHERE message_id = ?", (message_id,)
                ).fetchone()
                if existing:
                    return existing["id"]

            cursor = conn.execute(
                "INSERT INTO tasks (input, message_id, status, repo_path, model) VALUES (?, ?, ?, ?, ?)",
                (task_input, message_id, initial_status, kwargs.get('repo_path'), kwargs.get('model'))
            )
            task_id = cursor.lastrowid
            
            if depends_on:
                conn.executemany(
                    "INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_id) VALUES (?, ?)",
                    [(task_id, dep_id) for dep_id in depends_on]
                )
                
            return task_id

    def claim_next(self) -> dict | None:
        """认领下一个 pending 任务，标记为 running"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            task = conn.execute("""
                SELECT * FROM tasks
                WHERE status = 'pending' AND attempts < max_attempts
                AND id NOT IN (
                    SELECT task_id FROM task_dependencies
                    JOIN tasks AS parent ON parent.id = task_dependencies.depends_on_id
                    WHERE parent.status != 'done'
                )
                ORDER BY created_at ASC
                LIMIT 1
            """).fetchone()

            if not task:
                return None

            conn.execute("""
                UPDATE tasks
                SET status = 'running',
                    started_at = datetime('now', 'localtime'),
                    attempts = attempts + 1
                WHERE id = ?
            """, (task["id"],))

            return dict(task)

    def update_progress(self, task_id: int, step: str):
        """更新任务进度"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            conn.execute(
                "UPDATE tasks SET progress = ? WHERE id = ?",
                (step, task_id)
            )

    def get_daily_seq(self, task_id: int) -> int:
        """获取任务在当天的序号（第几个任务）"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            task = conn.execute(
                "SELECT created_at FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if not task:
                return task_id

            # 获取同一天中比这个任务更早的任务数
            date_str = task["created_at"][:10]  # 取日期部分 YYYY-MM-DD
            row = conn.execute(
                "SELECT COUNT(*) as seq FROM tasks WHERE created_at LIKE ? AND id <= ?",
                (f"{date_str}%", task_id)
            ).fetchone()
            return row["seq"]

    def complete(self, task_id: int, branch: str, commit_sha: str = None, result: str = None):
        """标记任务完成"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("""
                UPDATE tasks
                SET status = 'done',
                    branch = ?,
                    commit_sha = ?,
                    result = ?,
                    finished_at = datetime('now', 'localtime')
                WHERE id = ?
            """, (branch, commit_sha, result, task_id))

    def fail(self, task_id: int, error: str):
        """标记任务失败"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("""
                UPDATE tasks
                SET status = 'failed',
                    error = ?,
                    finished_at = datetime('now', 'localtime')
                WHERE id = ?
            """, (error, task_id))

    def cancel(self, task_id: int) -> bool:
        """取消 pending 或 review 状态的任务"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                UPDATE tasks
                SET status = 'canceled',
                    finished_at = datetime('now', 'localtime')
                WHERE id = ? AND status IN ('pending', 'review')
            """, (task_id,))
            return cursor.rowcount > 0

    def approve(self, task_id: int) -> bool:
        """批准处于 review 状态的任务，将其转为 pending"""
        with self._connect() as conn:
            cursor = conn.execute("""
                UPDATE tasks
                SET status = 'pending'
                WHERE id = ? AND status = 'review'
            """, (task_id,))
            return cursor.rowcount > 0

    def reject(self, task_id: int) -> bool:
        """拒绝处于 review 状态的任务"""
        with self._connect() as conn:
            cursor = conn.execute("""
                UPDATE tasks
                SET status = 'rejected',
                    finished_at = datetime('now', 'localtime')
                WHERE id = ? AND status = 'review'
            """, (task_id,))
            return cursor.rowcount > 0

    def get(self, task_id: int) -> dict | None:
        """获取单个任务"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            task = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            return dict(task) if task else None

    def list_recent(self, limit: int = 10) -> list[dict]:
        """列出最近的任务"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            tasks = conn.execute("""
                SELECT t.*,
                       (SELECT COUNT(*) FROM task_dependencies d
                        JOIN tasks p ON p.id = d.depends_on_id
                        WHERE d.task_id = t.id AND p.status != 'done') as blocking_deps
                FROM tasks t
                ORDER BY t.created_at DESC
                LIMIT ?
            """, (limit,)).fetchall()
            
            result = []
            for t in tasks:
                t_dict = dict(t)
                if t_dict['status'] == 'pending' and t_dict.get('blocking_deps', 0) > 0:
                    t_dict['status'] = 'blocked'
                result.append(t_dict)
            return result

    def recover_stale(self, timeout_minutes: int = 10):
        """恢复超时的 running 任务（worker 启动时调用）"""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cutoff = datetime.now() - timedelta(minutes=timeout_minutes)
            conn.execute("""
                UPDATE tasks
                SET status = 'pending'
                WHERE status = 'running'
                AND started_at < ?
            """, (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))


# 测试代码
if __name__ == "__main__":
    store = TaskStore()

    # 测试入队
    task_id = store.enqueue("测试任务", "msg_001")
    print(f"入队: task_id={task_id}")

    # 测试幂等
    task_id2 = store.enqueue("测试任务", "msg_001")
    print(f"幂等: task_id={task_id2} (应该和上面一样)")

    # 测试认领
    task = store.claim_next()
    print(f"认领: {task}")

    # 测试完成
    store.complete(task_id, "dev/task-1", "abc123", "成功")
    print(f"完成: {store.get(task_id)}")