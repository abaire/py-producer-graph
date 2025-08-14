import asyncio
from typing import Any

import pytest

from producer_graph._pipeline import Pipeline, PipelineNode
from producer_graph._processor import MultiOutputProcessor, StandardProcessor


class MockProcessor(StandardProcessor):
    def __init__(self, transform=lambda x: x, **kwargs):
        super().__init__(transform, **kwargs)
        self.processed_items = []

    async def run(self, *args, **kwargs):
        # In tests, we don't need the full run loop, just the transform
        pass

    async def _process_item(self, item: Any) -> Any:
        processed = await super()._process_item(item)
        self.processed_items.append(processed)
        return processed


@pytest.fixture
def single_node_pipeline_def():
    return [
        PipelineNode(
            name="A",
            processor=StandardProcessor(lambda x: x + 1),
            num_workers=1,
            max_queue_size=10,
        )
    ]


@pytest.fixture
def multi_node_pipeline_def():
    return [
        PipelineNode(
            name="A",
            processor=StandardProcessor(lambda x: x + 1),
            num_workers=2,
            max_queue_size=10,
        ),
        PipelineNode(
            name="B",
            processor=StandardProcessor(lambda x: x * 2),
            num_workers=1,
            max_queue_size=10,
            input_node="A",
        ),
        PipelineNode(
            name="C",
            processor=MultiOutputProcessor(lambda x: range(x)),
            num_workers=1,
            max_queue_size=10,
            input_node="B",
        ),
    ]


def test_pipeline_init_no_nodes():
    with pytest.raises(ValueError, match="at least one node"):
        Pipeline([])


def test_pipeline_init_duplicate_nodes():
    nodes = [
        PipelineNode("A", MockProcessor(), 1, 1),
        PipelineNode("A", MockProcessor(), 1, 1),
    ]
    with pytest.raises(ValueError, match="specified multiple times"):
        Pipeline(nodes)


def test_pipeline_init_invalid_input_node():
    nodes = [PipelineNode("A", MockProcessor(), 1, 1, input_node="B")]
    with pytest.raises(ValueError, match="invalid input"):
        Pipeline(nodes)


def test_pipeline_init_multiple_source_nodes():
    nodes = [
        PipelineNode("A", MockProcessor(), 1, 1),
        PipelineNode("B", MockProcessor(), 1, 1),
    ]
    with pytest.raises(ValueError, match="one source node"):
        Pipeline(nodes)


def test_pipeline_init_no_source_nodes():
    nodes = [
        PipelineNode("A", MockProcessor(), 1, 1, input_node="B"),
        PipelineNode("B", MockProcessor(), 1, 1, input_node="A"),
    ]
    with pytest.raises(ValueError, match="one source node"):
        Pipeline(nodes)


@pytest.mark.asyncio
async def test_pipeline_run_data_flow(multi_node_pipeline_def):
    # Add a sink node to collect results
    results = []

    async def sink_transform(x):
        results.append(x)

    sink_node = PipelineNode(
        name="sink",
        processor=StandardProcessor(sink_transform),
        num_workers=1,
        max_queue_size=100,
        input_node="C",
    )
    multi_node_pipeline_def.append(sink_node)

    pipeline = Pipeline(multi_node_pipeline_def)
    initial_data = [1, 2, 3, 4]

    run_task = asyncio.create_task(pipeline.run(initial_data))
    await asyncio.sleep(0.2)  # Allow time for processing
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    # (1+1)*2 = 4 -> range(4) = 0,1,2,3
    # (2+1)*2 = 6 -> range(6) = 0,1,2,3,4,5
    # (3+1)*2 = 8 -> range(8) = 0,1,2,3,4,5,6,7
    # (4+1)*2 = 10 -> range(10) = 0,1,2,3,4,5,6,7,8,9
    expected_sum = sum(range(4)) + sum(range(6)) + sum(range(8)) + sum(range(10))
    assert sum(results) == expected_sum
    # The order is not guaranteed due to multiple workers, so we check the sum
    # and the count of items.
    expected_count = 4 + 6 + 8 + 10
    assert len(results) == expected_count
