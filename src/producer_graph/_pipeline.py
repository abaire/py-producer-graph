from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from producer_graph._processor import Processor

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineNode:
    """Defines a node in the processing graph.

    name: The identifier for this node type, used by other nodes to link as a producer.
    processor: Processor instance used to perform data transformations in this node.
    num_workers: The maximum number of parallel instances of this processor.
    max_queue_size: The maximum number of outputs that may exist at any point in time.
    input_node: The `name` of a PipelineNode instance that provides the inputs to instances of this processor.
    """

    name: str
    processor: Processor
    num_workers: int
    max_queue_size: int
    input_node: str | None = None


class Pipeline:
    """Orchestrates a linear processing chain of producer-consumer nodes."""

    def __init__(self, nodes: Iterable[PipelineNode]):
        self._nodes: dict[str, PipelineNode] = {}
        for node in nodes:
            if node.name in self._nodes:
                msg = f"Node '{node.name}' specified multiple times."
                raise ValueError(msg)
            self._nodes[node.name] = node

        self._queues: dict[str, asyncio.Queue] = {}
        self._source_node_name: str | None = None
        self._shutdown_barriers: dict[str, asyncio.Barrier] = {}
        self._build_chain()

    def _build_chain(self):
        """Initializes queues and validates the linear chain structure."""
        if not self._nodes:
            msg = "Pipeline must have at least one node."
            raise ValueError

        source_nodes = []
        for name, node in self._nodes.items():
            if node.input_node:
                if node.input_node not in self._nodes:
                    msg = f"Node '{name}' has an invalid input: '{node.input_node}'"
                    raise ValueError

                producer_node = self._nodes[node.input_node]
                self._queues[producer_node.name] = asyncio.Queue(maxsize=producer_node.max_queue_size)
                self._shutdown_barriers[producer_node.name] = asyncio.Barrier(producer_node.num_workers)
            else:
                source_nodes.append(name)

        if len(source_nodes) != 1:
            msg = f"Pipeline chain must have exactly one source node (a node with no input). Found: {len(source_nodes)}"
            raise ValueError(msg)
        self._source_node_name = source_nodes[0]

    async def run(self, initial_data: Iterable[Any]):
        """Executes the pipeline with a given set of initial inputs for the source node."""
        all_tasks = []
        if not self._source_node_name:
            msg = "Pipeline is misconfigured, no source node exists."
            raise ValueError(msg)
        source_node = self._nodes[self._source_node_name]

        work_chunks = [list(initial_data)[i :: source_node.num_workers] for i in range(source_node.num_workers)]

        for name, node in self._nodes.items():
            input_queue = self._queues.get(node.input_node)
            output_queue = self._queues.get(node.name)
            barrier = self._shutdown_barriers.get(node.name)

            is_source = name == self._source_node_name
            node_work_chunks = work_chunks if is_source else [None] * node.num_workers

            for i in range(node.num_workers):
                worker_name = f"{name}-worker-{i}"
                task = asyncio.create_task(
                    node.processor.run(
                        worker_name=name,
                        initial_work=node_work_chunks[i],
                        input_queue=input_queue,
                        output_queue=output_queue,
                        shutdown_barrier=barrier,
                    ),
                    name=worker_name,
                )
                all_tasks.append(task)

        await asyncio.gather(*all_tasks)
