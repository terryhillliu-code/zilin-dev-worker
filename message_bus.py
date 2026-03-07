import sqlite3
import os
import time
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager

DB_PATH = Path("/Users/liufang/zhiwei-dev/messages.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sender        TEXT NOT NULL,
    topic         TEXT NOT NULL,
    content       TEXT NOT NULL,
    metadata      TEXT, -- JSON encoded dict
    status        TEXT DEFAULT 'pending', -- pending, sent, failed
    retry_count   INTEGER DEFAULT 0,
    max_retries   INTEGER DEFAULT 3,
    error         TEXT,
    created_at    TEXT DEFAULT (datetime('now', 'localtime')),
    sent_at       TEXT
);

CREATE INDEX IF NOT EXISTS idx_status ON messages(status);
CREATE INDEX IF NOT EXISTS idx_topic ON messages(topic);
"""

class MessageBus:
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
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def publish(self, sender: str, topic: str, content: str, metadata: dict = None) -> int:
        import json
        meta_str = json.dumps(metadata, ensure_ascii=False) if metadata else None
        
        with self._connect() as conn:
            cursor = conn.execute(
                "INSERT INTO messages (sender, topic, content, metadata) VALUES (?, ?, ?, ?)",
                (sender, topic, content, meta_str)
            )
            return cursor.lastrowid

    def consume_pending(self, topic: str = None, limit: int = 10) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            
            query = "SELECT * FROM messages WHERE status = 'pending' AND retry_count < max_retries"
            params = []
            
            if topic:
                query += " AND topic = ?"
                params.append(topic)
                
            query += " ORDER BY created_at ASC LIMIT ?"
            params.append(limit)
            
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
            
            if not rows:
                return []
                
            # 锁定这些消息，防止并发消费
            ids = [r["id"] for r in rows]
            placeholders = ",".join(["?"] * len(ids))
            conn.execute(
                f"UPDATE messages SET status = 'processing' WHERE id IN ({placeholders})",
                ids
            )
            
            return [dict(r) for r in rows]

    def mark_sent(self, msg_id: int):
        with self._connect() as conn:
            conn.execute(
                "UPDATE messages SET status = 'sent', sent_at = datetime('now', 'localtime') WHERE id = ?",
                (msg_id,)
            )

    def mark_failed(self, msg_id: int, error: str):
        with self._connect() as conn:
            conn.execute("""
                UPDATE messages 
                SET status = 'pending', 
                    retry_count = retry_count + 1,
                    error = ?
                WHERE id = ?
            """, (error, msg_id))
            
            # 检查是否超过重试次数
            conn.execute("""
                UPDATE messages
                SET status = 'failed'
                WHERE id = ? AND retry_count >= max_retries
            """, (msg_id,))
