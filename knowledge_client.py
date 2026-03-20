#!/usr/bin/env python3
"""
知识库客户端 v3 - zhiwei-rag 统一接口
迁移自 v2 (ChromaDB + klib.db) 到 zhiwei-rag (LanceDB + 本地 Embedding)
"""
import os
import re
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

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


class KnowledgeClient:
    """
    知识库客户端 v3

    迁移说明：
    - v2: ChromaDB + klib.db + library.db 三路检索
    - v3: zhiwei-rag API (LanceDB + 本地 Embedding + Reranker)

    接口保持不变：
    - should_trigger_rag(task_input) -> bool
    - get_context(task_input, top_k) -> str
    """

    # zhiwei-rag 配置
    RAG_VENV_PYTHON = Path.home() / "zhiwei-rag" / "venv" / "bin" / "python3"
    RAG_BRIDGE = Path.home() / "zhiwei-rag" / "bridge.py"

    def __init__(self):
        pass  # 无需初始化，使用子进程调用

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
        通过 zhiwei-rag bridge 子进程检索知识库上下文。

        Args:
            task_input: 任务描述
            top_k: 返回结果数量

        Returns:
            格式化的上下文字符串，可直接拼接到 Prompt 中
        """
        if not self.RAG_VENV_PYTHON.exists() or not self.RAG_BRIDGE.exists():
            logger.warning("zhiwei-rag 环境不存在，返回空上下文")
            return ""

        keywords = _extract_keywords(task_input)
        logger.info(f"RAG 提取关键词: {keywords}")

        try:
            # 使用关键词作为查询
            query_text = " ".join(keywords[:3])

            # 通过子进程调用 bridge.py
            import subprocess
            import json

            result = subprocess.run(
                [
                    str(self.RAG_VENV_PYTHON),
                    str(self.RAG_BRIDGE),
                    "retrieve",
                    query_text,
                    "--top-k", str(top_k)
                ],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(self.RAG_BRIDGE.parent)
            )

            if result.returncode != 0:
                logger.warning(f"RAG 检索失败: {result.stderr}")
                return ""

            # 解析 JSON 输出
            try:
                results = json.loads(result.stdout)
            except json.JSONDecodeError:
                logger.warning(f"RAG 输出解析失败: {result.stdout[:100]}")
                return ""

            if not results:
                return ""

            # 格式化输出（兼容 v2 格式）
            context = "\n=== [Reference Knowledge from zhiwei-rag] ===\n"
            for r in results:
                text = r.get('text', '') or r.get('raw_text', '')
                source = r.get('source', 'unknown')
                track = r.get('track', 'vector')
                score = r.get('score', 0)

                # 截断过长的文本
                content_preview = text[:400] if text else ""

                source_label = {
                    "vector": "语义搜索",
                    "fts": "全文检索",
                    "graph": "知识图谱"
                }.get(track, track)

                score_str = f" (相关度: {score:.4f})" if score else ""

                # 尝试提取标题
                title = source if source != "unknown" else "知识库条目"
                if len(content_preview) > 20:
                    title = content_preview[:30].replace("\n", " ") + "..."

                context += f"\n📖 {title} [{source_label}{score_str}]\n"
                context += f"{content_preview}\n---\n"

            return context

        except subprocess.TimeoutExpired:
            logger.warning("RAG 检索超时 (30s)")
            return ""
        except Exception as e:
            logger.warning(f"RAG 检索失败: {e}")
            return ""


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

    print("\n=== 测试 RAG 触发判断 ===")
    test_cases = [
        ("帮我修改 API 接口", True),
        ("写一个简单的 hello world", False),
        ("请帮我设计一个新的 API 接口用于任务调度系统架构优化", True),
    ]
    for inp, expected in test_cases:
        result = client.should_trigger_rag(inp)
        status = "✅" if result == expected else "❌"
        print(f"  {status} '{inp[:30]}...' -> {result}")

    print("\n=== 测试综合检索 ===")
    ctx = client.get_context("请帮我设计一个新的 API 接口用于任务调度")
    print(ctx if ctx else "(无结果)")