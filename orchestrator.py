#!/usr/bin/env python3
"""
轻量级谋微编排引擎 (Multi-Agent Orchestrator)
用于 zhiwei-dev 并发工作流。

输入: 宏大需求 (如 "开发一个登录界面")
输出: 自动将拆解后的子任务 DAG 网络直接灌入 tasks.db。

用法:
    python3 orchestrator.py "开发一个包含前后端验证的完整登录页面"
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# 尝试导入 openai（百炼兼容）
try:
    from openai import OpenAI
except ImportError:
    print("错误: 需要安装 openai 库. (pip install openai)")
    sys.exit(1)

# 添加环境变量
sys.path.insert(0, str(Path(__file__).parent))
from knowledge_client import KnowledgeClient
from model_router import get_best_model

# 模型矩阵配置 (Phase 4.4.2 & 4.5.1)
MODEL_PLANNER = "qwen3-max-2026-01-23"  # 顶级规划
ANTHROPIC_AUTH_TOKEN = os.environ.get("ANTHROPIC_AUTH_TOKEN", "")
ANTHROPIC_BASE_URL = "https://coding.dashscope.aliyuncs.com/v1"

# [Phase 4.5.1] 硬编码防御性指令模板，省去大模型再润色开销
PROMPT_TEMPLATE = """
[Zhiwei Dev Worker 指令]
==== 历史经验参考 ====
{lessons}
==== 任务行动指南 ====
1. 严禁修改未授权或未涉及的文件。
2. 保持代码整洁，不引入不相关的重构。
3. 请参考上述历史经验，尽量避免触发已知陷阱。
==== 具体执行动作 ====
{raw_prompt}
"""

def optimize_prompt(raw_prompt: str, context_knowledge: str = "") -> str:
    """使用硬编码模板包装指令，替代代价高昂的二次 LLM 调用"""
    lessons = context_knowledge if context_knowledge else "无参考记录。"
    return PROMPT_TEMPLATE.format(lessons=lessons, raw_prompt=raw_prompt)


def generate_dag_plan(request: str) -> dict:
    """调用大模型，生成 DAG 任务图"""
    if not ANTHROPIC_AUTH_TOKEN:
        print("请设置环境变量 ANTHROPIC_AUTH_TOKEN")
        sys.exit(1)

    # v32.5: 引入 RAG 强化 Planner
    knowledge = KnowledgeClient()
    historical_lessons = knowledge.get_context(f"[BugFix] {request}", top_k=3)
    
    client = OpenAI(
        api_key=ANTHROPIC_AUTH_TOKEN,
        base_url=ANTHROPIC_BASE_URL
    )

    system_prompt = f"""你是知微系统的顶级架构师 (PlannerAgent)。
你应该使用 {MODEL_PLANNER} 的深度逻辑，将用户的需求拆解为多个独立的子任务 DAG 网络。

==== 领域历史避坑知识 ====
{historical_lessons}

要求：
1. 输出必须是合法的 JSON 对象。
2. 任务间的依赖必须是一棵有效的 DAG。
3. [Token 瘦身原则]: worker_prompt 指令应该极度聚焦“做什么”与“搜哪些文件”！
   - 不要生成长篇大论的实现细节代码。
   - 采用“指针式”指示，如：“搜索 library.db 中关于 X 的记录并依照其建议修改”。把阅读代码的冗长劳动留给后续 Worker，你只需点明方向！
"""

    print(f"🧠 正在进行多仓库协同规划 (Model: {MODEL_PLANNER})...")
    if historical_lessons:
        print("   (已注入 RAG 避坑经验，采用轻量化指针传递)")
    
    try:
        response = client.chat.completions.create(
            model=MODEL_PLANNER,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": request}
            ],
            response_format={"type": "json_object"},
            temperature=0.2
        )
        
        content = response.choices[0].message.content
        plan = json.loads(content.strip())
        
        # [Phase 4.4.2] 指令二次进化
        for task in plan.get("subtasks", []):
            task["worker_prompt"] = optimize_prompt(task["worker_prompt"], historical_lessons)
            
        return plan
    except Exception as e:
        print(f"❌ 大模型规划失败: {e}")
        sys.exit(1)


def topological_sort(subtasks: list) -> list:
    """按照依赖关系进行拓扑排序，并确保无环"""
    task_map = {t["id"]: t for t in subtasks}
    in_degree = {t["id"]: 0 for t in subtasks}
    adj = {t["id"]: [] for t in subtasks}

    for t in subtasks:
        for dep in t.get("depends_on", []):
            if dep in adj:
                adj[dep].append(t["id"])
                in_degree[t["id"]] += 1
            else:
                print(f"⚠️ 警告: 任务 {t['id']} 依赖了不存在的任务 {dep}，将忽略该依赖。")

    queue = [tid for tid in in_degree if in_degree[tid] == 0]
    sorted_tasks = []

    while queue:
        current = queue.pop(0)
        sorted_tasks.append(task_map[current])
        for neighbor in adj[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(sorted_tasks) != len(subtasks):
        print("❌ 错误: 任务依赖关系中存在环 (Cycle detected)！")
        sys.exit(1)

    return sorted_tasks


def main():
    parser = argparse.ArgumentParser(description="Multi-Agent Orchestrator for zhiwei-dev")
    parser.add_argument("request", help="宏大的自然语言需求描述")
    parser.add_argument("--dry-run", action="store_true", help="只输出 JSON 计划，不入库")
    args = parser.parse_args()

    plan = generate_dag_plan(args.request)
    subtasks = plan.get("subtasks", [])

    if not subtasks:
        print("❌ 规划器没有返回任何子任务。")
        sys.exit(1)

    print(f"\n✅ 成功拆解为 {len(subtasks)} 个子任务。正在进行依赖分析...")
    sorted_tasks = topological_sort(subtasks)

    if args.dry_run:
        print("\n--- 预演模式 (Dry-run): 任务编排顺序 ---")
        for i, t in enumerate(sorted_tasks):
            print(f"{i+1}. [{t['id']}] (依赖: {t.get('depends_on', [])}) => {t['worker_prompt'][:50]}...")
        sys.exit(0)

    print("\n📦 正在将子任务网络灌入 Tasks.db ...")
    store = TaskStore()
    
    # 因为已经按照拓扑排序，保证插入时其依赖项已经存在于 db_id_map 中
    db_id_map = {}
    
    for t in sorted_tasks:
        # 将字符串的 id 依赖映射为实际的数据库 ID
        real_depends_on = []
        for dep in t.get("depends_on", []):
            if dep in db_id_map:
                real_depends_on.append(db_id_map[dep])

        # [Phase 4.4.3] 动态模型分发
        allocated_model = get_best_model(t['worker_prompt'])

        # 获取更连贯的 task input 描述，带上前置信息
        worker_input = f"[子任务 {t['id']}]\n上下文需求: {plan.get('request', args.request)}\n---\n具体指令: {t['worker_prompt']}"

        db_id = store.enqueue(
            task_input=worker_input,
            initial_status='pending',
            depends_on=real_depends_on,
            repo_path=t.get("repo_path"),
            model=allocated_model
        )
        db_id_map[t["id"]] = db_id
        print(f"  + 已创建子任务 '{t['id']}' [Model: {allocated_model}] -> 数据库任务 ID: #{db_id}")

    print("\n🚀 所有任务已入列！并行的 Worker 进程将按照 DAG 顺序自动提走执行。")


if __name__ == "__main__":
    main()
