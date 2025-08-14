# ruff: noqa: T201 `print` found

from __future__ import annotations

import asyncio
import logging
import time

from producer_graph import Pipeline, PipelineNode


async def fetch_source_file(filename: str) -> dict[str, str]:
    """Simulates an async I/O operation."""
    print(f"🌎 Fetching {filename}...")
    await asyncio.sleep(0.05)
    return {"path": filename, "data": "raw_data"}


def transcode(source_data: dict[str, str]) -> dict[str, str]:
    """Simulates a blocking, CPU-bound operation."""
    path = source_data["path"]
    print(f"🔩 Transcoding {path}...")
    time.sleep(1.0)
    source_data["transcoded"] = "transcoded_data"
    return source_data


def constrained_analyze(source_data: dict[str, str]) -> dict[str, str]:
    path = source_data["path"]
    transcoded_data = source_data["transcoded"]
    print(f"🚀 Analyzing transcoded data for {path}...")
    time.sleep(1.5)
    return {"path": path, "report": f"processed '{transcoded_data}'"}


async def upload_to_s3(data: dict) -> None:
    """Simulates a final async I/O operation (a sink)."""
    path = data["path"]
    print(f"☁️  Uploading report for {path} to S3...")
    await asyncio.sleep(0.3)


async def main():
    """Defines the graph and runs the pipeline."""

    logging.basicConfig(level=logging.DEBUG)

    nodes = [
        PipelineNode(
            name="fetch",
            transform=fetch_source_file,
            num_workers=5,
            max_queue_size=10,
        ),
        PipelineNode(
            name="transcode",
            transform=transcode,
            long_running=True,
            num_workers=4,
            max_queue_size=8,
            input_node="fetch",
        ),
        PipelineNode(
            name="constrained_analyze",
            transform=transcode,
            long_running=True,
            num_workers=1,
            max_queue_size=3,
            input_node="transcode",
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
    initial_filenames = [f"input_{i}.mp4" for i in range(8)]

    print("🚀 Starting pipeline...")
    await pipeline.run(initial_filenames)
    print("✅ Pipeline finished.")


if __name__ == "__main__":
    asyncio.run(main())
