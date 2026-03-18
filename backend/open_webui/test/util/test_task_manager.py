import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "utils" / "task_manager.py"
)
SPEC = importlib.util.spec_from_file_location("task_manager", MODULE_PATH)
task_manager = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(task_manager)

BackgroundTaskManager = task_manager.BackgroundTaskManager
build_background_task_manager = task_manager.build_background_task_manager


@pytest.mark.asyncio
async def test_background_task_manager_spawn_updates_and_cleans_up():
    event_emitter = AsyncMock()
    app_state = SimpleNamespace()
    task_manager = BackgroundTaskManager(
        app_state=app_state,
        chat_id="chat-id",
        message_id="message-id",
        event_emitter=event_emitter,
    )

    async def worker(updater):
        await updater.update("Updated content")

    task = task_manager.spawn(worker)
    await task

    event_emitter.assert_awaited_once_with(
        {
            "type": "replace",
            "data": {
                "chat_id": "chat-id",
                "message_id": "message-id",
                "content": "Updated content",
            },
        }
    )
    assert getattr(app_state, "tool_background_tasks", None) == set()


@pytest.mark.asyncio
async def test_background_task_manager_spawn_shows_failure_message_on_error():
    event_emitter = AsyncMock()
    app_state = SimpleNamespace()
    task_manager = BackgroundTaskManager(
        app_state=app_state,
        chat_id="chat-id",
        message_id="message-id",
        event_emitter=event_emitter,
    )

    async def worker(_updater):
        raise RuntimeError("boom")

    task = task_manager.spawn(worker)
    await task

    event_emitter.assert_awaited_once_with(
        {
            "type": "replace",
            "data": {
                "chat_id": "chat-id",
                "message_id": "message-id",
                "content": "⚠️ Background task failed: boom",
            },
        }
    )
    assert getattr(app_state, "tool_background_tasks", None) == set()


def test_build_background_task_manager_uses_tool_context():
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    event_emitter = AsyncMock()

    task_manager = build_background_task_manager(
        request,
        {
            "__chat_id__": "chat-id",
            "__message_id__": "message-id",
            "__event_emitter__": event_emitter,
        },
    )

    assert task_manager is not None
    assert task_manager.chat_id == "chat-id"
    assert task_manager.message_id == "message-id"


def test_build_background_task_manager_returns_none_without_message_context():
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    assert build_background_task_manager(request, {"__chat_id__": "chat-id"}) is None
