"""执行后端基类"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class ExecuteResult:
    success: bool
    stdout: str
    stderr: str
    returncode: int


class DevBackend(ABC):

    @abstractmethod
    def execute(self, task: str, workspace: str, log_path: str) -> ExecuteResult:
        """
        在指定 workspace 中执行开发任务

        Args:
            task: 任务描述
            workspace: 工作目录路径
            log_path: 日志输出路径

        Returns:
            ExecuteResult
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """后端名称"""
        pass