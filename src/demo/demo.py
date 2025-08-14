# ruff: noqa: T201 `print` found

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from producer_graph import Pipeline, PipelineNode

if TYPE_CHECKING:
    from collections.abc import Generator


async def fetch_source_file(filename: str) -> dict[str, str]:
    """Stage 1: Fetches a single source item."""
    print(f"🌎 [Fetch]     Starting: {filename}")
    await asyncio.sleep(0.05)
    return {"id": filename, "data": "raw_data"}


def split_data(source_data: dict[str, str]) -> Generator[dict[str, str], None, None]:
    """
    Stage 2: A synchronous generator that takes one item and yields two processed items.
    """
    path = source_data["id"]
    print(f"✂️  [Split]     Processing: {path}")
    time.sleep(0.2)
    # Yield the first output for this input
    yield {"id": f"{path}-part1", "original_id": path, "processed": "split_data_A"}
    # Yield the second output for this input
    yield {"id": f"{path}-part2", "original_id": path, "processed": "split_data_B"}


def constrained_analyze(source_data: dict[str, str]) -> dict[str, str]:
    """Stage 3: A resource-constrained analysis step."""
    path = source_data["id"]
    processed_data = source_data["processed"]
    print(f"🚀 [Analyze]   Starting: {path}")
    time.sleep(1.5)
    source_data["report"] = f"analyzed_{processed_data}"
    return source_data


async def upload_to_s3(data: dict) -> None:
    """Stage 4: A final sink node to upload the report."""
    path = data["id"]
    print(f"☁️  [Upload]    Uploading report for {path}...")
    await asyncio.sleep(0.3)


async def main():
    """Defines and runs the pipeline."""

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    nodes = [
        PipelineNode(
            name="fetch",
            transform=fetch_source_file,
            num_workers=5,
            max_queue_size=10,
        ),
        PipelineNode(
            name="split",
            multi_transform=split_data,
            long_running=True,
            num_workers=2,
            max_queue_size=8,
            input_node="fetch",
        ),
        PipelineNode(
            name="constrained_analyze",
            transform=constrained_analyze,
            long_running=True,
            num_workers=1,
            max_queue_size=3,
            input_node="split",
        ),
        PipelineNode(
            name="upload",
            transform=upload_to_s3,
            num_workers=4,
            max_queue_size=0,
            input_node="constrained_analyze",
        ),
    ]

    pipeline = Pipeline(nodes)
    initial_filenames = [f"input_{i}.dat" for i in range(8)]

    print("🚀 Starting pipeline...")
    await pipeline.run(initial_filenames)
    print("✅ Pipeline finished.")


if __name__ == "__main__":
    asyncio.run(main())
