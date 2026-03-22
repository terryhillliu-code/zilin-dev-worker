#!/usr/bin/env python3
"""
研发验收自动化校验脚本 (v2.0)

验证任务是否满足「无证据、不交付」原则：
- L1: 命令行返回日志
- L2: 测试脚本执行结果
- L3: Spec 文档对齐

用法:
    # 校验指定任务
    python verify_evidence.py --task 42

    # 校验最近完成的任务
    python verify_evidence.py --recent

    # 生成验收报告
    python verify_evidence.py --report
"""

import json
import os
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

# 路径配置
TASK_DB = Path.home() / "zhiwei-dev" / "tasks.db"
SPEC_DIR = Path.home() / "zhiwei-docs" / "specs"
REPORT_DIR = Path.home() / "zhiwei-docs" / "reports"


@dataclass
class EvidenceCheck:
    """证据检查结果"""
    level: str  # L1/L2/L3
    name: str
    passed: bool
    details: str


@dataclass
class TaskVerification:
    """任务验收结果"""
    task_id: int
    input_text: str
    status: str
    has_evidence: bool
    evidence_level: str
    checks: List[EvidenceCheck]
    spec_aligned: bool
    ready_for_review: bool


def get_task_store():
    """获取 TaskStore 实例"""
    sys.path.insert(0, str(Path.home() / "zhiwei-dev"))
    from task_store import TaskStore
    return TaskStore()


def check_l1_evidence(task: Dict) -> EvidenceCheck:
    """
    检查 L1 证据：命令行返回日志

    证据格式：在 verification_evidence 中包含 command 和 output
    """
    evidence = task.get("verification_evidence", "")

    if not evidence:
        return EvidenceCheck(
            level="L1",
            name="命令行日志",
            passed=False,
            details="缺少 verification_evidence 字段"
        )

    # 检查是否包含命令输出
    has_command = "command:" in evidence.lower() or "$" in evidence
    has_output = "output:" in evidence.lower() or "✓" in evidence or "✅" in evidence

    if has_command and has_output:
        return EvidenceCheck(
            level="L1",
            name="命令行日志",
            passed=True,
            details="包含命令和输出"
        )
    elif has_command or has_output:
        return EvidenceCheck(
            level="L1",
            name="命令行日志",
            passed=True,
            details="部分证据存在（建议补充完整命令和输出）"
        )
    else:
        return EvidenceCheck(
            level="L1",
            name="命令行日志",
            passed=False,
            details="证据格式不符合要求"
        )


def check_l2_evidence(task: Dict) -> EvidenceCheck:
    """
    检查 L2 证据：测试脚本执行结果

    证据格式：包含 test 或 verify 关键词，且有通过/失败标记
    """
    evidence = task.get("verification_evidence", "")
    output = task.get("output", "")

    combined = f"{evidence}\n{output}".lower()

    # 检查是否有测试相关内容
    test_keywords = ["test", "verify", "验证", "测试", "pytest", "assert"]
    has_test = any(kw in combined for kw in test_keywords)

    # 检查是否有结果标记
    pass_markers = ["pass", "✓", "✅", "成功", "通过"]
    fail_markers = ["fail", "❌", "失败", "错误"]

    has_pass = any(m in combined for m in pass_markers)
    has_fail = any(m in combined for m in fail_markers)

    if has_test and (has_pass or has_fail):
        return EvidenceCheck(
            level="L2",
            name="测试脚本结果",
            passed=True,
            details="包含测试脚本执行结果"
        )
    elif has_test:
        return EvidenceCheck(
            level="L2",
            name="测试脚本结果",
            passed=True,
            details="包含测试内容（建议添加明确结果标记）"
        )
    else:
        return EvidenceCheck(
            level="L2",
            name="测试脚本结果",
            passed=False,
            details="未发现测试脚本证据（复杂任务需要 L2 证据）"
        )


def check_l3_spec_alignment(task: Dict) -> EvidenceCheck:
    """
    检查 L3 证据：Spec 文档对齐

    检查是否涉及 Spec 文件修改
    """
    input_text = task.get("input", "")
    output = task.get("output", "")
    evidence = task.get("verification_evidence", "")

    combined = f"{input_text}\n{output}\n{evidence}".lower()

    # 检查是否涉及 Spec 相关关键词
    spec_keywords = ["spec", "规范", "架构", "specification", "rag_retrieval", "video_distillation"]
    needs_spec = any(kw in combined for kw in spec_keywords)

    if not needs_spec:
        return EvidenceCheck(
            level="L3",
            name="Spec 文档对齐",
            passed=True,
            details="不涉及 Spec 文档修改"
        )

    # 检查是否有 Spec 文档更新记录
    spec_update_markers = ["spec 更新", "更新 spec", "文档同步", "spec 同步"]
    has_spec_update = any(m in combined for m in spec_update_markers)

    if has_spec_update:
        return EvidenceCheck(
            level="L3",
            name="Spec 文档对齐",
            passed=True,
            details="已同步 Spec 文档"
        )
    else:
        return EvidenceCheck(
            level="L3",
            name="Spec 文档对齐",
            passed=False,
            details="涉及 Spec 相关修改，但未发现文档更新记录"
        )


def determine_evidence_level(task: Dict) -> str:
    """
    判断任务所需的证据等级

    - 简单任务：L1
    - 中等任务：L1 + L2
    - 复杂任务：L1 + L2 + L3
    """
    input_text = task.get("input", "").lower()

    # 复杂任务关键词
    complex_keywords = ["架构", "重构", "新功能", "spec", "规范", "系统"]
    # 中等任务关键词
    medium_keywords = ["修复", "优化", "改进", "测试", "验证"]

    if any(kw in input_text for kw in complex_keywords):
        return "L3"
    elif any(kw in input_text for kw in medium_keywords):
        return "L2"
    else:
        return "L1"


def verify_task(task_id: int) -> TaskVerification:
    """
    验证单个任务的证据完整性
    """
    store = get_task_store()
    task = store.get(task_id)

    if not task:
        raise ValueError(f"任务 #{task_id} 不存在")

    # 执行检查
    checks = [
        check_l1_evidence(task),
        check_l2_evidence(task),
        check_l3_spec_alignment(task),
    ]

    # 判断所需证据等级
    required_level = determine_evidence_level(task)

    # 判断是否准备就绪
    l1_passed = checks[0].passed
    l2_passed = checks[1].passed
    l3_passed = checks[2].passed

    ready = l1_passed
    if required_level in ["L2", "L3"]:
        ready = ready and l2_passed
    if required_level == "L3":
        ready = ready and l3_passed

    return TaskVerification(
        task_id=task_id,
        input_text=task.get("input", ""),
        status=task.get("status", "unknown"),
        has_evidence=bool(task.get("verification_evidence")),
        evidence_level=required_level,
        checks=checks,
        spec_aligned=l3_passed,
        ready_for_review=ready
    )


def generate_report(verification: TaskVerification) -> str:
    """生成验收报告"""
    lines = [
        f"# 任务验收报告 #{verification.task_id}",
        "",
        f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**状态**: {verification.status}",
        f"**所需证据等级**: {verification.evidence_level}",
        "",
        "## 证据检查",
        "",
        "| 等级 | 检查项 | 状态 | 详情 |",
        "|------|--------|------|------|",
    ]

    for check in verification.checks:
        status = "✅" if check.passed else "❌"
        lines.append(f"| {check.level} | {check.name} | {status} | {check.details} |")

    lines.extend([
        "",
        "## 验收结论",
        "",
        f"**准备就绪**: {'✅ 是' if verification.ready_for_review else '❌ 否'}",
        "",
    ])

    if verification.ready_for_review:
        lines.append("任务已满足验收标准，可以提交审核。")
    else:
        lines.append("**待补充证据**:")
        for check in verification.checks:
            if not check.passed:
                lines.append(f"- [{check.level}] {check.name}: {check.details}")

    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print("用法: verify_evidence.py --task <id> | --recent | --report")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "--task":
        task_id = int(sys.argv[2]) if len(sys.argv) > 2 else None
        if not task_id:
            print("请指定任务 ID")
            sys.exit(1)

        verification = verify_task(task_id)
        print(generate_report(verification))

        if not verification.ready_for_review:
            sys.exit(1)

    elif cmd == "--recent":
        store = get_task_store()
        tasks = store.list_recent(5)

        print("=== 最近任务验收状态 ===\n")
        for task in tasks:
            task_id = task.get("id")
            try:
                verification = verify_task(task_id)
                status = "✅" if verification.ready_for_review else "❌"
                print(f"#{task_id} [{status}] {task.get('input', '')[:50]}...")
            except Exception as e:
                print(f"#{task_id} [⚠️] 检查失败: {e}")

    elif cmd == "--report":
        if len(sys.argv) < 3:
            print("请指定任务 ID: verify_evidence.py --report <task_id>")
            sys.exit(1)

        task_id = int(sys.argv[2])
        verification = verify_task(task_id)
        report = generate_report(verification)

        # 保存报告
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        report_file = REPORT_DIR / f"verification_{task_id}_{datetime.now().strftime('%Y%m%d')}.md"
        with open(report_file, "w") as f:
            f.write(report)

        print(f"报告已保存: {report_file}")
        print(report)

    else:
        print(f"未知命令: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()