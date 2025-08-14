# ruff: noqa: T201 `print` found
# ruff: noqa: S311 Standard pseudo-random generators are not suitable for cryptographic purposes

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import TYPE_CHECKING

from producer_graph import Pipeline, batch_node, multitransform_node, standard_node

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

    for i in range(random.randint(1, 3)):
        yield {"id": f"{path}-part{i}", "original_id": path, "processed": f"split_data_{i}"}


def constrained_analyze(source_data: dict[str, str]) -> dict[str, str]:
    """Stage 3: A resource-constrained analysis step."""
    path = source_data["id"]
    processed_data = source_data["processed"]
    print(f"🚀 [Analyze]   Starting: {path}")
    time.sleep(random.uniform(0.4, 1.25))
    source_data["report"] = f"analyzed_{processed_data}"
    return source_data


async def upload_to_s3(data: list[dict[str, str]]) -> None:
    """Stage 4: A final sink node to upload the report."""

    paths = ", ".join(item["id"] for item in data)
    print(f"☁️  [Upload]    Uploading report for {paths}...")
    await asyncio.sleep(0.3)


async def main():
    """Defines and runs the pipeline."""

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    random.seed(123)

    nodes = [
        standard_node("fetch", fetch_source_file, num_workers=5, max_queue_size=10),
        multitransform_node(
            name="split",
            transform=split_data,
            spawn_thread=True,
            num_workers=2,
            max_queue_size=8,
            input_node="fetch",
        ),
        standard_node(
            name="constrained_analyze",
            transform=constrained_analyze,
            spawn_thread=True,
            num_workers=1,
            max_queue_size=3,
            input_node="split",
        ),
        batch_node(
            name="upload_batcher",
            batch_size=4,
            timeout_seconds=0.8,
            input_node="constrained_analyze",
        ),
        standard_node(
            name="upload",
            transform=upload_to_s3,
            num_workers=4,
            max_queue_size=0,
            input_node="upload_batcher",
        ),
    ]

    pipeline = Pipeline(nodes)
    initial_filenames = [f"input_{i}.dat" for i in range(9)]

    print("🚀 Starting pipeline...")
    await pipeline.run(initial_filenames)
    print("✅ Pipeline finished.")


if __name__ == "__main__":
    asyncio.run(main())
