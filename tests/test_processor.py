import asyncio
from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest

from producer_graph._processor import (
    DONE_SENTINEL,
    BatchingProcessor,
    MultiOutputProcessor,
    StandardProcessor,
)


async def run_processor_test(processor, input_data, *, initial_work=False) -> list[Any]:
    """Helper function to run a processor and collect its output."""
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()

    if not initial_work:
        for item in input_data:
            await input_queue.put(item)
        await input_queue.put(DONE_SENTINEL)

    task = asyncio.create_task(
        processor.run(
            "test_worker",
            input_queue=input_queue if not initial_work else None,
            output_queue=output_queue,
            initial_work=input_data if initial_work else None,
        )
    )

    results = []
    while True:
        item = await output_queue.get()
        if item is DONE_SENTINEL:
            break
        results.append(item)

    await task
    return results


@pytest.mark.asyncio
async def test_standard_processor_sync():
    """Test StandardProcessor with a synchronous transform."""
    processor = StandardProcessor(lambda x: x * 2)
    input_data = [1, 2, 3]
    results = await run_processor_test(processor, input_data)
    assert results == [2, 4, 6]


@pytest.mark.asyncio
async def test_standard_processor_async():
    """Test StandardProcessor with an asynchronous transform."""

    async def async_transform(x):
        await asyncio.sleep(0.01)
        return x * 2

    processor = StandardProcessor(async_transform)
    input_data = [1, 2, 3]
    results = await run_processor_test(processor, input_data)
    assert results == [2, 4, 6]


@pytest.mark.asyncio
async def test_standard_processor_threaded():
    """Test StandardProcessor with a blocking transform in a thread."""

    def blocking_transform(x):
        # time.sleep(0.01) # Simulate blocking I/O
        return x * 2

    processor = StandardProcessor(blocking_transform, spawn_thread=True)
    input_data = [1, 2, 3]
    results = await run_processor_test(processor, input_data)
    assert results == [2, 4, 6]


@pytest.mark.asyncio
async def test_multi_output_processor_iterable():
    """Test MultiOutputProcessor with a transform returning an iterable."""

    def multi_transform(x) -> Generator[int, None, None]:
        yield from range(x)

    processor = MultiOutputProcessor(multi_transform)
    input_data = [1, 2, 3]
    results = await run_processor_test(processor, input_data)
    assert results == [0, 0, 1, 0, 1, 2]


@pytest.mark.asyncio
async def test_multi_output_processor_async_iterable():
    """Test MultiOutputProcessor with a transform returning an async iterable."""

    async def async_multi_transform(x) -> AsyncGenerator[int, None]:
        for i in range(x):
            await asyncio.sleep(0.01)
            yield i

    processor = MultiOutputProcessor(async_multi_transform)
    input_data = [1, 2, 3]
    results = await run_processor_test(processor, input_data)
    assert results == [0, 0, 1, 0, 1, 2]


def test_batching_processor_invalid_init():
    """Test BatchingProcessor initialization with invalid arguments."""
    with pytest.raises(ValueError, match="batch_size"):
        BatchingProcessor(batch_size=1)
    with pytest.raises(ValueError, match="timeout"):
        BatchingProcessor(batch_size=2, timeout_seconds=-1)


@pytest.mark.asyncio
async def test_batching_processor_by_size():
    """Test BatchingProcessor that batches items by size."""
    processor = BatchingProcessor(batch_size=3)
    input_data = [1, 2, 3, 4, 5, 6, 7]
    results = await run_processor_test(processor, input_data)
    assert results == [[1, 2, 3], [4, 5, 6], [7]]


@pytest.mark.asyncio
async def test_batching_processor_by_timeout():
    """Test BatchingProcessor that emits batches due to a timeout."""
    processor = BatchingProcessor(batch_size=5, timeout_seconds=0.01)
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()

    task = asyncio.create_task(processor.run("test_worker", input_queue=input_queue, output_queue=output_queue))

    await input_queue.put(1)
    await input_queue.put(2)

    # Wait for the timeout to trigger the batch emission
    result1 = await output_queue.get()
    assert result1 == [1, 2]

    await input_queue.put(3)
    await input_queue.put(DONE_SENTINEL)

    result2 = await output_queue.get()
    assert result2 == [3]

    assert await output_queue.get() is DONE_SENTINEL
    await task


@pytest.mark.asyncio
async def test_batching_processor_with_initial_work():
    """Test BatchingProcessor with initial_work provided."""
    processor = BatchingProcessor(batch_size=2)
    input_data = [1, 2, 3, 4, 5]
    results = await run_processor_test(processor, input_data, initial_work=True)
    assert results == [[1, 2], [3, 4], [5]]


@pytest.mark.asyncio
async def test_batching_processor_custom_function():
    """Test BatchingProcessor with a custom batching function."""

    def custom_batching(item, batch):
        if batch is None:
            batch = set()
        batch.add(item)
        return batch

    processor = BatchingProcessor(batch_size=3, batching_function=custom_batching)
    input_data = [1, 2, 1, 3, 2, 4]
    results = await run_processor_test(processor, input_data)
    assert results == [{1, 2, 3}, {2, 4}]
