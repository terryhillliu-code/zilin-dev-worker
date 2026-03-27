#!/usr/bin/env python3
"""
CriticAgent (自省代理)
用于自动分析知微系统执行失败的任务，并将修复经验沉淀至知识库。
"""
import os
import sys
import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
import re
from task_store import TaskStore

# 尝试导入 openai（百炼兼容）
try:
    from openai import OpenAI
except ImportError:
    print("错误: 需要安装 openai 库. (pip install openai)")
    sys.exit(1)

# 配置
TASKS_DB = Path(__file__).parent / "tasks.db"
LIBRARY_DB_PATH = Path(os.path.expanduser("~/Documents/clawdbot download/knowledge-library/library.db"))
ARTIFACTS_BASE = Path(__file__).parent / "artifacts"

# 百炼 API 配置 (Phase 4.4.1: 使用顶级推理模型)
MODEL_CRITIC = "qwen3-max-2026-01-23"
# 使用统一加载后的环境变量 (v57.0)
DASHSCOPE_API_KEY = os.environ.get("DASHSCOPE_API_KEY", "")
ANTHROPIC_BASE_URL = "https://coding.dashscope.aliyuncs.com/v1"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class CriticAgent:
    def __init__(self):
        self.api_key = DASHSCOPE_API_KEY
        if not self.api_key:
            logger.error("环境变量 DASHSCOPE_API_KEY 未设置")
        
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=ANTHROPIC_BASE_URL
        )

    def analyze_task(self, task_id: int):
        """分析指定 ID 的任务失败原因，支持多维打分"""
        logger.info(f"🔍 [Phase 4.4] 正在深度复盘任务 #{task_id} (使用 {MODEL_CRITIC})...")
        
        # 1. 提取任务上下文
        conn = sqlite3.connect(str(TASKS_DB))
        conn.row_factory = sqlite3.Row
        task = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        conn.close()
        
        if not task:
            logger.error(f"任务 #{task_id} 不存在")
            return

        # 2. 读取关联日志
        artifacts_dir = ARTIFACTS_BASE / str(task_id)
        error_log = ""
        run_log = ""
        
        if (artifacts_dir / "error.log").exists():
            error_log = (artifacts_dir / "error.log").read_text(errors='ignore')
        if (artifacts_dir / "run.log").exists():
            raw_run = (artifacts_dir / "run.log").read_text(errors='ignore')
            run_log = self._clean_run_log(raw_run)
        
        # 3. 构造深度复盘 Prompt
        system_prompt = f"""你是知微系统的顶级 CriticAgent (高级自省专家)。
你的职责是使用 {MODEL_CRITIC} 的顶级能力，对一次失败的开发任务进行彻查。

你必须基于以下维度进行打分 (0-100)：
1. logic_score: 逻辑正确性（代码逻辑是否符合人类意图）。
2. safety_score: 安全性（是否涉及越权、凭证泄露、破坏性操作）。
3. quality_score: 代码工程质量（冗余度、可读性、是否符合 PEP8）。

请以 JSON 格式输出：
{{
  "concept": "[BugFix] 模式化的标题",
  "scores": {{
    "logic": 0,
    "safety": 0,
    "quality": 0
  }},
  "failure_cause": "根本原因深度分析",
  "fix_suggestion": "具体的、带代码示例的修复方案，供 Worker 重试参考",
  "can_retry": true/false
}}
"""
        user_prompt = f"""
### 任务背景
- 需求: {task['input']}
- 状态: {task['status']}
- 原始报错: {task['error']}

### 执行现场
--- 运行日志 (关键片段) ---
{run_log[-2000:] if run_log else "无日志数据"}

--- 错误堆栈/stderr ---
{error_log}
"""

        try:
            response = self.client.chat.completions.create(
                model=MODEL_CRITIC,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            
            content = response.choices[0].message.content
            analysis = json.loads(content)
            logger.info(f"✅ 复盘完成 | 逻辑得分: {analysis['scores']['logic']} | 修复建议: {analysis['concept']}")
            
            # 4. 写入知识库与沉淀
            self._save_to_library(analysis, task_id)
            
            # 5. [新增] 触发自动重试 (若分值过低且允许重试)
            if analysis.get('can_retry') and analysis['scores']['logic'] < 80:
                self._trigger_repair_retry(task_id, analysis['fix_suggestion'])
            
        except Exception as e:
            logger.error(f"分析失败: {e}")

    def _clean_run_log(self, raw_log: str) -> str:
        """[Phase 4.5.2] 清洗运行日志，剔除长篇的思考噪音以节省 Token"""
        if not raw_log:
            return ""
        
        # 1. 移除 ANSI 颜色字符
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        cleaned = ansi_escape.sub('', raw_log)
        
        # 2. 移除 "Thinking..." 或类似冗长的内部盘点块 (假设被 ``` 包裹或者特定特征)
        # 此处采用保守策略：剔除连续超过 10 行的重复模式，或只截取最后 1500 个字符
        # 为了稳定，直接截取尾部最核心的 1500 字符，并清理空行
        lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
        short_log = "\n".join(lines[-100:])  # 只取最后 100 行有效动作
        
        return short_log[:1500]

    def _trigger_repair_retry(self, old_task_id: int, suggestion: str):
        """将带有修复建议的任务重新入列 (Self-healing 闭环)"""
        logger.info(f"🔁 任务 #{old_task_id} 表现不佳，已根据 Critic 建议申请重修...")
        
        try:
            store = TaskStore()
            old_task = store.get(old_task_id)
            if not old_task:
                return

            new_input = (
                f"### [Self-Healing Retry] 自动修复指令\n"
                f"这是对先前失败任务 #{old_task_id} 的自动重试。\n\n"
                f"#### Critic 修复建议 (请务必遵循):\n"
                f"{suggestion}\n\n"
                f"#### 原始任务指令:\n"
                f"{old_task['input']}"
            )
            
            new_id = store.enqueue(
                task_input=new_input,
                repo_path=old_task.get('repo_path')
            )
            logger.info(f"🚀 已创建修复任务 #{new_id}，正在等待 Worker 认领。")
        except Exception as e:
            logger.error(f"创建修复任务失败: {e}")

    def _save_to_library(self, analysis: dict, task_id: int):
        """将深度分析沉淀至 library.db"""
        if not LIBRARY_DB_PATH.exists():
            logger.warning(f"library.db 未找到: {LIBRARY_DB_PATH}")
            return

        try:
            conn = sqlite3.connect(str(LIBRARY_DB_PATH))
            explanation = (
                f"### Task #{task_id} Multi-Dimensional Analysis\n"
                f"- Logic Score: {analysis['scores']['logic']}\n"
                f"- Safety Score: {analysis['scores']['safety']}\n"
                f"- Quality Score: {analysis['scores']['quality']}\n\n"
                f"#### Root Cause\n{analysis['failure_cause']}\n\n"
                f"#### Fix Suggestion\n{analysis['fix_suggestion']}"
            )
            conn.execute("""
                INSERT INTO knowledge_items (concept, explanation, created_at)
                VALUES (?, ?, ?)
            """, (
                analysis['concept'],
                explanation,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
            conn.commit()
            conn.close()
            logger.info("💾 深度复盘知识已入库。")
        except Exception as e:
            logger.error(f"存库失败: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python3 critic_agent.py <task_id>")
        sys.exit(1)
        
    tid = int(sys.argv[1])
    agent = CriticAgent()
    agent.analyze_task(tid)
