from __future__ import annotations

# ruff: noqa: F401 imported but unused; consider removing, adding to `__all__`, or using a redundant alias
from producer_graph._pipeline import Pipeline, PipelineNode
from producer_graph._processor import (
    BasicTransformCallable,
    MultiOutputProcessor,
    MultiTransformCallbale,
    Processor,
    StandardProcessor,
)


def standard_node(
    name: str,
    transform: BasicTransformCallable,
    num_workers: int = 4,
    max_queue_size: int = 10,
    input_node: str | None = None,
    *,
    spawn_thread: bool = False,
) -> PipelineNode:
    """Constructs a PipelineNode configured with a standard (1:1) input transformer."""
    return PipelineNode(
        name=name,
        processor=StandardProcessor(transform, spawn_thread=spawn_thread),
        num_workers=num_workers,
        max_queue_size=max_queue_size,
        input_node=input_node,
    )


def multitransform_node(
    name: str,
    transform: MultiTransformCallbale,
    num_workers: int = 4,
    max_queue_size: int = 10,
    input_node: str | None = None,
    *,
    spawn_thread: bool = False,
) -> PipelineNode:
    """Constructs a PipelineNode configured with a 1-to-many input transformer."""
    return PipelineNode(
        name=name,
        processor=MultiOutputProcessor(transform, spawn_thread=spawn_thread),
        num_workers=num_workers,
        max_queue_size=max_queue_size,
        input_node=input_node,
    )
