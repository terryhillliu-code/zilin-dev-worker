#!/usr/bin/env python3
"""
知识库客户端 v2 - 深度 RAG 集成
支持：向量语义搜索 (ChromaDB) + FTS5 全文搜索 (klib.db) + 概念检索 (library.db)
"""
import os
import re
import sqlite3
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# === 环境配置 (Host Mac 路径) ===
# library.db: 概念条目与文档元信息
LIBRARY_DB_PATH = Path(os.environ.get(
    "KLIB_DB_PATH",
    os.path.expanduser("~/Documents/clawdbot download/knowledge-library/library.db")
))
# klib.db: FTS5 全文搜索（Docker 映射到宿主机的实际路径）
KLIB_FTS_PATH = Path(os.environ.get(
    "KLIB_FTS_PATH",
    os.path.expanduser("~/clawdbot-docker/workspace/data/klib.db")
))
# ChromaDB: 向量语义搜索
CHROMADB_PATH = Path(os.environ.get(
    "CHROMADB_PATH",
    os.path.expanduser("~/Documents/clawdbot download/knowledge-library/chromadb")
))

# Embedding 配置 (复用 OpenClaw DashScope)
EMBEDDING_API_KEY = os.environ.get(
    "EMBEDDING_API_KEY",
    "" # 生产环境严禁硬编码
)
EMBEDDING_BASE_URL = os.environ.get(
    "EMBEDDING_BASE_URL",
    "https://dashscope.aliyuncs.com/compatible-mode/v1"
)
EMBEDDING_MODEL = "text-embedding-v3"
EMBEDDING_DIMENSION = 1024

# RAG 触发配置 (中英文)
# 核心技术词：直接触发
CORE_TECHNICAL = {
    "api", "架构", "设计", "规范", "接口", "schema", "config", "配置",
    "模块", "组件", "原理", "集成", "迁移", "数据库", "chromadb", "sqlite"
}
# 启发式通用词：仅在句子较长或包含核心词时辅助触发
GENERIC_HEURISTIC = {
    "如何", "怎么", "为什么", "方案", "历史", "记录", "参考", "文档"
}

RAG_TRIGGER_MIN_LENGTH = 30  # 触发 RAG 的最小描述长度


def _extract_keywords(task_input: str) -> list[str]:
    """
    从任务描述中智能提取搜索关键词。
    策略：
      1. 去除常见的命令性词汇（"请", "帮我", "需要" 等）
      2. 提取中文核心名词和英文技术术语
      3. 保留有意义的 2-6 字词组
    """
    # 去除命令性前缀
    noise = ["请", "帮我", "需要", "我想", "你来", "开始",
             "执行", "完成", "实现", "修改", "添加", "创建"]
    cleaned = task_input
    for n in noise:
        cleaned = cleaned.replace(n, " ")

    keywords = []

    # 提取英文技术术语 (如 API, ChromaDB, scheduler 等)
    en_terms = re.findall(r'[A-Za-z_][A-Za-z0-9_.-]{2,}', cleaned)
    keywords.extend([t.lower() for t in en_terms[:5]])

    # 提取中文名词短语 (2-6 字)
    cn_phrases = re.findall(r'[\u4e00-\u9fff]{2,6}', cleaned)
    # 过滤掉过于通用的词
    stopwords = {"任务", "功能", "模块", "系统", "进行", "使用", "通过", "支持", "相关"}
    cn_phrases = [p for p in cn_phrases if p not in stopwords]
    keywords.extend(cn_phrases[:5])

    # 如果提取失败，回退到取前 30 字符
    if not keywords:
        keywords = [task_input[:30].replace("\n", " ")]

    return keywords


class EmbeddingCache:
    """基于 SQLite 的本地 Embedding 缓存，节省百炼额度"""
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS emb_cache (
                    text_hash TEXT PRIMARY KEY,
                    text_content TEXT,
                    embedding_blob BLOB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def get(self, text: str) -> list | None:
        import hashlib
        import json
        text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
        with sqlite3.connect(str(self.db_path)) as conn:
            row = conn.execute(
                "SELECT embedding_blob FROM emb_cache WHERE text_hash = ?",
                (text_hash,)
            ).fetchone()
            if row:
                return json.loads(row[0])
        return None

    def set(self, text: str, embedding: list):
        import hashlib
        import json
        text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO emb_cache (text_hash, text_content, embedding_blob) VALUES (?, ?, ?)",
                (text_hash, text, json.dumps(embedding))
            )


class KnowledgeClient:
    def __init__(self):
        self._chroma_client = None
        self.cache = EmbeddingCache(Path(__file__).parent / "rag_cache.db")

    def _get_embedding(self, texts: list) -> list:
        """调用 DashScope 获取 Embedding (带本地缓存)"""
        # 简单起见，这里只处理包含一个 text 的列表（search_vector 使用场景）
        if len(texts) == 1:
            cached = self.cache.get(texts[0])
            if cached:
                logger.info(f"✨ Embedding 缓存命中: {texts[0][:20]}...")
                return [cached]

        try:
            import requests
        except ImportError:
            logger.warning("requests 模块未安装，无法获取 embedding")
            return []

        resp = requests.post(
            f"{EMBEDDING_BASE_URL}/embeddings",
            headers={"Authorization": f"Bearer {EMBEDDING_API_KEY}", "Content-Type": "application/json"},
            json={"model": EMBEDDING_MODEL, "input": texts, "dimension": EMBEDDING_DIMENSION},
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        embeddings = [item["embedding"] for item in sorted(data["data"], key=lambda x: x["index"])]
        
        # 存入缓存
        if len(texts) == 1 and embeddings:
            self.cache.set(texts[0], embeddings[0])
            
        return embeddings

    # ----- 搜索层 1: 概念检索 (library.db) -----
    def search_concepts(self, query: str, top_k: int = 3) -> list[dict]:
        """在 library.db 的 knowledge_items 中搜索概念条目"""
        if not LIBRARY_DB_PATH.exists():
            logger.debug(f"library.db 不存在: {LIBRARY_DB_PATH}")
            return []

        conn = sqlite3.connect(str(LIBRARY_DB_PATH))
        conn.row_factory = sqlite3.Row
        try:
            results = []
            cursor = conn.execute("""
                SELECT concept, explanation
                FROM knowledge_items
                WHERE concept LIKE ? OR explanation LIKE ?
                LIMIT ?
            """, (f"%{query}%", f"%{query}%", top_k))
            for r in cursor.fetchall():
                results.append({"title": r[0], "content": r[1], "source": "concept"})

            if len(results) < top_k:
                cursor = conn.execute("""
                    SELECT title, description
                    FROM documents
                    WHERE title LIKE ? OR description LIKE ?
                    LIMIT ?
                """, (f"%{query}%", f"%{query}%", top_k - len(results)))
                for r in cursor.fetchall():
                    results.append({"title": r[0], "content": r[1] or "", "source": "document"})

            return results
        except Exception as e:
            logger.warning(f"概念检索错误: {e}")
            return []
        finally:
            conn.close()

    # ----- 搜索层 2: FTS5 全文搜索 (klib.db) -----
    def search_fts(self, query: str, top_k: int = 3) -> list[dict]:
        """在 klib.db 中执行 FTS5 全文搜索"""
        if not KLIB_FTS_PATH.exists():
            logger.debug(f"klib.db 不存在: {KLIB_FTS_PATH}")
            return []

        conn = sqlite3.connect(str(KLIB_FTS_PATH))
        conn.row_factory = sqlite3.Row
        try:
            # 检查 FTS5 表是否存在
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name = 'books_fts'
            """)
            has_fts = cursor.fetchone() is not None

            results = []
            if has_fts:
                safe_query = f'"{query}"'
                cursor = conn.execute("""
                    SELECT title, author, toc, summary
                    FROM books_fts
                    WHERE books_fts MATCH ?
                    ORDER BY rank
                    LIMIT ?
                """, (safe_query, top_k))
                for r in cursor.fetchall():
                    content = f"作者: {r[1] or ''}\n目录: {r[2] or ''}\n摘要: {r[3] or ''}"
                    results.append({"title": r[0], "content": content, "source": "fts5"})

            # FTS5 无结果时回退到 LIKE
            if not results:
                cursor = conn.execute("""
                    SELECT title, file_path, category
                    FROM books
                    WHERE title LIKE ? OR category LIKE ?
                    LIMIT ?
                """, (f"%{query}%", f"%{query}%", top_k))
                for r in cursor.fetchall():
                    results.append({
                        "title": r[0],
                        "content": f"路径: {r[1]}\n分类: {r[2]}",
                        "source": "keyword"
                    })

            return results
        except Exception as e:
            logger.warning(f"FTS5 搜索错误: {e}")
            return []
        finally:
            conn.close()

    # ----- 搜索层 3: 向量语义搜索 (ChromaDB) -----
    def search_vector(self, query: str, top_k: int = 3) -> list[dict]:
        """ChromaDB 语义搜索"""
        try:
            import chromadb
        except ImportError:
            logger.debug("chromadb 未安装，跳过向量搜索")
            return []

        if not CHROMADB_PATH.exists():
            logger.debug(f"ChromaDB 目录不存在: {CHROMADB_PATH}")
            return []

        if self._chroma_client is None:
            self._chroma_client = chromadb.PersistentClient(path=str(CHROMADB_PATH))

        try:
            collection = self._chroma_client.get_collection("knowledge_base")
            if collection.count() == 0:
                return []

            query_embedding = self._get_embedding([query])
            if not query_embedding:
                return []

            results = collection.query(
                query_embeddings=[query_embedding[0]],
                n_results=min(top_k, collection.count()),
                include=["documents", "metadatas", "distances"]
            )

            items = []
            if results and results["documents"] and results["documents"][0]:
                for i, doc_text in enumerate(results["documents"][0]):
                    meta = results["metadatas"][0][i] if results["metadatas"] else {}
                    distance = results["distances"][0][i] if results["distances"] else 0
                    score = round(1 - distance, 4)
                    # 只保留相关性 > 0.3 的结果
                    if score > 0.3:
                        items.append({
                            "title": meta.get("title", "未知"),
                            "content": doc_text,
                            "score": score,
                            "source": "vector"
                        })
            return items
        except Exception as e:
            logger.warning(f"向量搜索错误: {e}")
            return []

    # ----- 综合检索入口 -----
    def should_trigger_rag(self, task_input: str) -> bool:
        """判断任务是否需要触发 RAG 检索 (P1 优化版)"""
        task_lower = task_input.lower()
        
        # 1. 包含核心技术词：直接触发
        if any(kw in task_lower for kw in CORE_TECHNICAL):
            return True
        
        # 2. 只有包含启发式词汇 且 长度达到阈值时 才触发
        if len(task_input) >= RAG_TRIGGER_MIN_LENGTH:
            if any(kw in task_lower for kw in GENERIC_HEURISTIC):
                return True
                
        return False

    def get_context(self, task_input: str, top_k: int = 3) -> str:
        """
        三路召回 + 去重 + 格式化输出。
        返回可直接拼接到 Prompt 中的上下文字符串。
        """
        keywords = _extract_keywords(task_input)
        logger.info(f"RAG 提取关键词: {keywords}")

        all_results = []
        seen_titles = set()

        for kw in keywords[:3]:  # 最多取前 3 个关键词搜索
            # 层1: 概念检索
            for r in self.search_concepts(kw, top_k=2):
                if r["title"] not in seen_titles:
                    seen_titles.add(r["title"])
                    all_results.append(r)

            # 层2: FTS5 全文搜索
            for r in self.search_fts(kw, top_k=2):
                if r["title"] not in seen_titles:
                    seen_titles.add(r["title"])
                    all_results.append(r)

        # 层3: 向量搜索（使用原始任务描述，语义更完整）
        query_text = " ".join(keywords[:3])
        for r in self.search_vector(query_text, top_k=top_k):
            if r["title"] not in seen_titles:
                seen_titles.add(r["title"])
                all_results.append(r)

        if not all_results:
            return ""

        # 按 score 排序（有 score 的优先）
        all_results.sort(key=lambda x: x.get("score", 0.5), reverse=True)

        # 格式化输出
        context = "\n=== [Reference Knowledge from OpenClaw] ===\n"
        for r in all_results[:top_k]:
            source_label = {
                "concept": "概念库", "document": "文档库",
                "fts5": "全文检索", "keyword": "关键词",
                "vector": "语义搜索"
            }.get(r["source"], r["source"])

            content_preview = r["content"][:400] if r.get("content") else ""
            score_str = f" (相关度: {r['score']})" if "score" in r else ""

            context += f"\n📖 {r.get('title', 'Untitled')} [{source_label}{score_str}]\n"
            context += f"{content_preview}\n---\n"

        return context


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = KnowledgeClient()

    print("=== 测试关键词提取 ===")
    test_inputs = [
        "请帮我重构 scheduler.py 的 API 接口设计",
        "给 supervisor_daemon 添加 stats.py 集成",
        "搜索一下 RISC-V 架构相关的文档",
    ]
    for inp in test_inputs:
        kw = _extract_keywords(inp)
        print(f"  输入: {inp[:40]}... -> 关键词: {kw}")

    print("\n=== 测试概念搜索 ===")
    print(client.search_concepts("架构"))

    print("\n=== 测试 FTS5 搜索 ===")
    print(client.search_fts("agent"))

    print("\n=== 测试综合检索 ===")
    ctx = client.get_context("请帮我设计一个新的 API 接口用于任务调度")
    print(ctx if ctx else "(无结果)")
