from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class PipelineContext:
    config: Any
    artifacts: Dict[str, Any] = field(default_factory=dict)


class PipelineStage:
    name = "stage"

    def run(self, context: PipelineContext, data: Optional[Any] = None) -> Any:
        raise NotImplementedError


class Pipeline:
    def __init__(self, stages: List[PipelineStage]) -> None:
        self.stages = stages

    def run(self, context: PipelineContext) -> Any:
        data = None
        for stage in self.stages:
            data = stage.run(context, data)
        return data
