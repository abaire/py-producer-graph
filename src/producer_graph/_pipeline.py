from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

DONE_SENTINEL = object()


@dataclass(frozen=True)
class PipelineNode:
    """Defines a node in the processing graph.

    name: The identifier for this node type, used by other nodes to link as a producer.
    transform: The method that should be executed by instances of this processor. This must produce a single output
            artifact per input. This is mutually exclusive with `multi_transform`.
    multi_transform: Method that should be executed by instances of this processor. This must produce an iterable of one
            or more outputs per input. This is mutually exclusive with `transform`.
    num_workers: The maximum number of parallel instances of this processor.
    max_queue_size: The maximum number of outputs that may exist at any point in time.
    input_node: The `name` of a PipelineNode instance that provides the inputs to instances of this processor.

    long_running: Indicates that the transform method takes considerable time and should be executed in a separate
                  thread.
    """

    name: str
    num_workers: int
    max_queue_size: int
    transform: Callable[[Any], Awaitable[Any] | Any] | None = None
    multi_transform: Callable[[Any], Iterable[Any] | AsyncIterable[Any]] | None = None
    input_node: str | None = None
    long_running: bool = False

    def __post_init__(self):
        """Validate that exactly one transform function is provided."""
        if (self.transform is None) == (self.multi_transform is None):
            msg = f"Node '{self.name}' must have exactly one of 'transform' or 'multi_transform' defined."
            raise ValueError(msg)


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

    async def _worker_loop(
        self, node: PipelineNode, shutdown_barrier: asyncio.Barrier | None, initial_work: Iterable[Any] | None = None
    ):
        """The core coroutine for a single worker task in a linear chain."""
        input_queue = self._queues.get(node.input_node) if node.input_node else None
        output_queue = self._queues.get(node.name)

        work_iterator = iter(initial_work) if initial_work else None

        if work_iterator and input_queue:
            msg = f"Nodes with initial work and an input producer are not supported: {node.name}"
            raise NotImplementedError(msg)
        if not (work_iterator or input_queue):
            msg = f"Node has no initial work and no input producer: {node.name}"
            raise ValueError(msg)

        while True:
            if work_iterator:
                try:
                    input_item = next(work_iterator)
                except StopIteration:
                    logging.debug("Source node %s processed all data.", node.name)
                    break
            else:
                input_item = await input_queue.get()
                if input_item is DONE_SENTINEL:
                    logging.debug("Node %s worker received DONE_SENTINEL.", node.name)
                    await input_queue.put(DONE_SENTINEL)
                    break

            result = await self._process_item(input_item, node)

            if output_queue:
                if not node.multi_transform:
                    await output_queue.put(result)
                elif isinstance(result, AsyncIterable):
                    async for sub_item in result:
                        await output_queue.put(sub_item)
                elif isinstance(result, Iterable) and not isinstance(result, str | bytes):
                    for sub_item in result:
                        await output_queue.put(sub_item)
                else:
                    await output_queue.put(result)

            if input_queue and input_item is not DONE_SENTINEL:
                input_queue.task_done()

        if output_queue:
            try:
                if not shutdown_barrier or not await shutdown_barrier.wait():
                    logger.debug("Last worker for node '%s' finished. Signaling done downstream.", node.name)
                    await output_queue.put(DONE_SENTINEL)
            except asyncio.BrokenBarrierError:
                # This can happen if the pipeline is cancelled.
                logger.warning("Shutdown barrier for node %s was broken.", node.name)

    async def _process_item(self, item: Any, node: PipelineNode) -> Any:
        """Helper to transform an item."""
        method = node.transform if node.transform else node.multi_transform

        if not method:
            msg = f"Node {node.name} has neither transform nor multi_transform set."
            raise ValueError(msg)

        if node.long_running:
            return await asyncio.to_thread(method, item)

        result = method(item)

        if asyncio.iscoroutine(result):
            return await result
        return result

    async def run(self, initial_data: Iterable[Any]):
        """Executes the pipeline with a given set of initial inputs for the source node."""
        all_tasks = []
        if not self._source_node_name:
            msg = "Pipeline is misconfigured, no source node exists."
            raise ValueError(msg)
        source_node = self._nodes[self._source_node_name]

        work_chunks = [list(initial_data)[i :: source_node.num_workers] for i in range(source_node.num_workers)]

        for name, node in self._nodes.items():
            barrier = self._shutdown_barriers.get(name)
            is_source = name == self._source_node_name
            node_work_chunks = work_chunks if is_source else [None] * node.num_workers

            for i in range(node.num_workers):
                task = asyncio.create_task(
                    self._worker_loop(node, shutdown_barrier=barrier, initial_work=node_work_chunks[i]),
                    name=f"{name}-worker-{i}",
                )
                all_tasks.append(task)

        await asyncio.gather(*all_tasks)
