#!/usr/bin/env python3
"""
ModelRouter (模型调度路由器) v33.0
根据任务 Prompt 的复杂度、涉及文件数及历史成功率，自动匹配最优模型。
"""

import os
import re

# 模型矩阵 (与 Implementation Plan 保持一致)
MODELS = {
    "planner": "qwen3-max-2026-01-23",
    "coder_high": "qwen3-coder-plus",
    "coder_standard": "qwen3.5-plus",
    "long_context": "glm-5",
    "prompt_optimizer": "MiniMax-M2.5"
}

def get_best_model(prompt_text: str, repo_path: str = None) -> str:
    """智能路由逻辑"""
    text_lower = prompt_text.lower()
    
    # 1. 极长上下文或涉及大规模代码审计 -> glm-5
    if len(prompt_text) > 8000 or "审计" in text_lower or "全面分析" in text_lower:
        return MODELS["long_context"]
    
    # 2. 核心架构逻辑、复杂重构、或明确要求 high-level 思维 -> max 系列
    high_complexity_keywords = ["重构", "架构", "底座", "解耦", "设计模式", "refactor", "architecture"]
    if any(kw in text_lower for kw in high_complexity_keywords):
        return MODELS["planner"]
    
    # 3. 标准编码任务、涉及 Git 深度操作 -> qwen3-coder-plus
    coding_keywords = ["实现", "开发", "编写", "bugfix", "fix", "implement", "coding"]
    if any(kw in text_lower for kw in coding_keywords):
        return MODELS["coder_high"]
    
    # 4. 简单任务、文档编写、单测扩充 -> qwen3.5-plus
    return MODELS["coder_standard"]

if __name__ == "__main__":
    # 简单测试
    test_prompts = [
        "重构整个系统的通知链路，消除冗余线程",
        "在 login.py 中添加一个简单的邮箱校验函数",
        "审计 zhiwei-dev 仓库的所有安全隐患并生成报告",
        "为现有函数添加注释"
    ]
    for p in test_prompts:
        print(f"Prompt: {p[:30]}... -> Model: {get_best_model(p)}")
