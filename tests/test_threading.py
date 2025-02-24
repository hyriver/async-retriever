from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from shapely import box

from async_retriever._utils import _AsyncLoopThread


def test_singleton_behavior():
    """Test that get_instance always returns the same instance."""
    instance1 = _AsyncLoopThread.get_instance()
    instance2 = _AsyncLoopThread.get_instance()
    assert instance1 is instance2
    assert instance1._running.is_set()
    assert instance1.is_alive()


def test_thread_safety():
    """Test thread-safe initialization under concurrent access."""

    def get_instance():
        return _AsyncLoopThread.get_instance()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_instance) for _ in range(20)]
        instances = [f.result() for f in futures]

    assert all(instance is instances[0] for instance in instances)
    assert instances[0]._running.is_set()


def test_shapely_thread_safety():
    """Test Shapely operations while async thread is running."""
    _ = _AsyncLoopThread.get_instance()

    def buffer_box():
        box_geom = box(0, 0, 1, 1)
        return box_geom.buffer(0.1).bounds

    results = []
    for _ in range(100):
        try:
            result = buffer_box()
            results.append(result)
        except Exception as e:  # noqa: PERF203
            pytest.fail(f"Shapely operation failed: {e}")

    assert len(results) == 100


def test_async_operations():
    """Test that the async loop actually works."""
    instance = _AsyncLoopThread.get_instance()

    async def async_task():
        await asyncio.sleep(0.1)
        return "success"

    future = asyncio.run_coroutine_threadsafe(async_task(), instance.loop)
    result = future.result(timeout=1)
    assert result == "success"


def test_concurrent_async_operations():
    """Test multiple async operations running concurrently."""
    instance = _AsyncLoopThread.get_instance()

    async def async_task(task_id: int) -> str:
        await asyncio.sleep(0.1)
        return f"task_{task_id}"

    futures = [asyncio.run_coroutine_threadsafe(async_task(i), instance.loop) for i in range(5)]

    results = [f.result(timeout=1) for f in futures]
    assert results == [f"task_{i}" for i in range(5)]


def test_cleanup():
    """Test cleanup behavior."""
    instance = _AsyncLoopThread.get_instance()
    assert instance._running.is_set()
    assert instance.is_alive()

    _AsyncLoopThread._cleanup()
    assert not instance._running.is_set()
    assert not instance.is_alive()

    new_instance = _AsyncLoopThread.get_instance()
    assert new_instance._running.is_set()
    assert new_instance.is_alive()
    assert new_instance is not instance


def test_stop_and_restart():
    """Test stopping and restarting the thread."""
    instance1 = _AsyncLoopThread.get_instance()
    assert instance1._running.is_set()
    assert instance1.is_alive()

    instance1.stop()
    assert not instance1._running.is_set()
    assert not instance1.is_alive()

    instance2 = _AsyncLoopThread.get_instance()
    assert instance2._running.is_set()
    assert instance2.is_alive()
    assert instance2 is not instance1

    async def async_task():
        await asyncio.sleep(0.1)
        return "success"

    future = asyncio.run_coroutine_threadsafe(async_task(), instance2.loop)
    result = future.result(timeout=1)
    assert result == "success"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Cleanup after each test."""
    yield
    if _AsyncLoopThread._instance is not None:
        _AsyncLoopThread._instance.stop()
    _AsyncLoopThread._instance = None
    _AsyncLoopThread._cleanup_registered = False
