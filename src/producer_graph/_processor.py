from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from collections.abc import AsyncIterable, Iterable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

DONE_SENTINEL = object()


class Processor(Protocol):
    """A protocol defining the interface for all worker logic classes."""

    async def run(
        self,
        worker_name: str,
        input_queue: asyncio.Queue,
        initial_work: Iterable[Any] | None = None,
        shutdown_barrier: asyncio.Barrier | None = None,
        output_queue: asyncio.Queue | None = None,
    ) -> None:
        """The main execution loop for a worker."""


type BasicTransformCallable = Callable[[Any], Awaitable[Any] | Any]
type MultiTransformCallbale = Callable[[Any], Iterable[Any] | AsyncIterable[Any]]
type TransformCallable = BasicTransformCallable | MultiTransformCallbale


class _ProcessorBase:
    def __init__(self, transform: TransformCallable, *, spawn_thread: bool = False):
        self.transform = transform
        self.spawn_thread = spawn_thread

    async def run(
        self,
        worker_name: str,
        input_queue: asyncio.Queue,
        initial_work: Iterable[Any] | None = None,
        shutdown_barrier: asyncio.Barrier | None = None,
        output_queue: asyncio.Queue | None = None,
    ) -> None:
        """The standard item-by-item processing loop."""
        work_iterator = iter(initial_work) if initial_work else None

        while True:
            if work_iterator:
                try:
                    input_item = next(work_iterator)
                except StopIteration:
                    logging.debug("Source node %s processed all data.", worker_name)
                    break
            else:
                input_item = await input_queue.get()
                if input_item is DONE_SENTINEL:
                    logging.debug("Node %s worker received DONE_SENTINEL.", worker_name)
                    await input_queue.put(DONE_SENTINEL)
                    break

            result = await self._process_item(input_item)

            if output_queue:
                await self._produce_output(result, output_queue)

            if input_queue and input_item is not DONE_SENTINEL:
                input_queue.task_done()

        await self._handle_done_event(worker_name, shutdown_barrier, output_queue)

    @abstractmethod
    async def _produce_output(self, result: Any, output_queue: asyncio.Queue):
        raise NotImplementedError

    async def _process_item(self, item: Any) -> Any:
        if self.spawn_thread:
            return await asyncio.to_thread(self.transform, item)

        result = self.transform(item)
        if asyncio.iscoroutine(result):
            return await result
        return result

    async def _handle_done_event(
        self, name: str, shutdown_barrier: asyncio.Barrier | None = None, output_queue: asyncio.Queue | None = None
    ) -> None:
        if not output_queue:
            return

        try:
            if not shutdown_barrier or not await shutdown_barrier.wait():
                logger.debug("Last worker for node '%s' finished. Signaling done downstream.", name)
                await output_queue.put(DONE_SENTINEL)

        except asyncio.BrokenBarrierError:
            # This can happen if the pipeline is cancelled.
            logger.warning("Shutdown barrier for node %s was broken.", name)


class StandardProcessor(_ProcessorBase):
    """Performs 1:1 transformations of input -> output."""

    def __init__(self, transform: BasicTransformCallable, *, spawn_thread: bool = False):
        super().__init__(transform=transform, spawn_thread=spawn_thread)

    async def _produce_output(self, result: Any, output_queue: asyncio.Queue):
        await output_queue.put(result)


class MultiOutputProcessor(_ProcessorBase):
    """Performs 1:n transformations of input -> output."""

    def __init__(self, transform: MultiTransformCallbale, *, spawn_thread: bool = False):
        super().__init__(transform=transform, spawn_thread=spawn_thread)

    async def _produce_output(self, result: Any, output_queue: asyncio.Queue):
        if isinstance(result, AsyncIterable):
            async for sub_item in result:
                await output_queue.put(sub_item)
        elif isinstance(result, Iterable) and not isinstance(result, str | bytes):
            for sub_item in result:
                await output_queue.put(sub_item)
        else:
            await output_queue.put(result)
