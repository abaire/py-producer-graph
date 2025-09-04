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
NO_OUTPUT = object()

type BasicTransformCallable = Callable[[Any], Awaitable[Any] | Any]
type MultiTransformCallbale = Callable[[Any], Iterable[Any] | AsyncIterable[Any]]
type TransformCallable = BasicTransformCallable | MultiTransformCallbale

# Folds a single input item into some container.
# E.g., fold_list(item: Any, state: list[Any]) -> tuple[list[any], bool]
# The second element of the returned tuple may be set to "True" to indicate that the batch must be considered complete,
# even if there is room for more elements.
type BatchingCallable = Callable[[Any, Any], tuple[Any, bool]]


class Processor(Protocol):
    """A protocol defining the interface for all worker logic classes."""

    async def run(
        self,
        worker_name: str,
        input_queue: asyncio.Queue | None = None,
        initial_work: Iterable[Any] | None = None,
        shutdown_barrier: asyncio.Barrier | None = None,
        output_queue: asyncio.Queue | None = None,
    ) -> None:
        """The main execution loop for a worker."""


class _ProcessorBase:
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


class _TransformProcessorBase(_ProcessorBase):
    def __init__(self, transform: TransformCallable, *, spawn_thread: bool = False):
        self.transform = transform
        self.spawn_thread = spawn_thread

    async def run(
        self,
        worker_name: str,
        input_queue: asyncio.Queue | None = None,
        initial_work: Iterable[Any] | None = None,
        shutdown_barrier: asyncio.Barrier | None = None,
        output_queue: asyncio.Queue | None = None,
    ) -> None:
        """The standard item-by-item processing loop."""
        if not (input_queue or initial_work):
            logging.debug("No work provided, exiting processor %s", worker_name)
            return

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

            if result is not NO_OUTPUT and output_queue:
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


class StandardProcessor(_TransformProcessorBase):
    """Performs 1:1 transformations of input -> output."""

    def __init__(self, transform: BasicTransformCallable, *, spawn_thread: bool = False):
        super().__init__(transform=transform, spawn_thread=spawn_thread)

    async def _produce_output(self, result: Any, output_queue: asyncio.Queue):
        await output_queue.put(result)


class MultiOutputProcessor(_TransformProcessorBase):
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


def _fold_list(value: Any, batch: list[Any] | None) -> tuple[list[Any], bool]:
    """Default batching function that simply adds items to a list."""
    if batch is None:
        batch = []

    batch.append(value)
    return batch, False


class BatchingProcessor(_ProcessorBase):
    """A processor that groups items into batches by size or timeout."""

    def __init__(
        self, batch_size: int, timeout_seconds: float = 0.0, batching_function: BatchingCallable | None = None
    ):
        if batch_size <= 1:
            msg = f"batch_size ({batch_size}) must be > 1"
            raise ValueError(msg)

        if timeout_seconds < 0.0:
            msg = f"timeout ({timeout_seconds}) must be >= 0"
            raise ValueError(msg)

        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.batching_function = batching_function if batching_function else _fold_list

    async def _produce_predefined_batches(self, initial_work: Iterable[Any], output_queue: asyncio.Queue) -> None:
        current_batch = None
        for item in initial_work:
            current_batch, force_complete = self.batching_function(item, current_batch)
            if force_complete or len(current_batch) >= self.batch_size:
                await output_queue.put(current_batch)
                current_batch = None

        if current_batch:
            await output_queue.put(current_batch)

        await output_queue.put(DONE_SENTINEL)

    async def _produce_batches(
        self,
        worker_name: str,
        input_queue: asyncio.Queue | None = None,
        shutdown_barrier: asyncio.Barrier | None = None,
        output_queue: asyncio.Queue | None = None,
    ) -> None:
        current_batch = None

        while True:
            force_complete = False
            if not current_batch:
                item = await input_queue.get()
                if item is DONE_SENTINEL:
                    break
                current_batch, force_complete = self.batching_function(item, current_batch)

            if force_complete or len(current_batch) >= self.batch_size:
                logger.debug("%s emitting full batch of %d", worker_name, len(current_batch))
                await output_queue.put(current_batch)
                current_batch = None
                continue

            timeout_task = None
            if self.timeout_seconds:
                timeout_task = asyncio.create_task(asyncio.sleep(self.timeout_seconds))

            item = None
            while len(current_batch) < self.batch_size:
                get_task = asyncio.create_task(input_queue.get())

                tasks_to_wait = [get_task]
                if timeout_task:
                    tasks_to_wait.append(timeout_task)

                done, pending = await asyncio.wait(tasks_to_wait, return_when=asyncio.FIRST_COMPLETED)

                if timeout_task and timeout_task in done:
                    get_task.cancel()
                    break

                item = get_task.result()
                if item is DONE_SENTINEL:
                    if timeout_task:
                        timeout_task.cancel()
                    await input_queue.put(DONE_SENTINEL)
                    break

                current_batch, force_complete = self.batching_function(item, current_batch)
                if force_complete:
                    break

            if current_batch:
                await output_queue.put(current_batch)
                current_batch = None

            if item is DONE_SENTINEL:
                break

        await self._handle_done_event(worker_name, shutdown_barrier, output_queue)

    async def run(
        self,
        worker_name: str,
        input_queue: asyncio.Queue | None = None,
        initial_work: Iterable[Any] | None = None,
        shutdown_barrier: asyncio.Barrier | None = None,
        output_queue: asyncio.Queue | None = None,
    ) -> None:
        """The standard item-by-item processing loop."""

        if not output_queue:
            msg = f"BatchProcessor {worker_name} was configured without an output queue."
            raise ValueError(msg)

        if initial_work:
            await self._produce_predefined_batches(initial_work, output_queue)
            return

        if not input_queue:
            msg = f"BatchProcessor {worker_name} has neither initial_work nor an input queue."
            raise ValueError(msg)

        await self._produce_batches(worker_name, input_queue, shutdown_barrier, output_queue)
