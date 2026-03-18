import asyncio
import inspect
import logging
from typing import Awaitable, Callable, Optional


log = logging.getLogger(__name__)


class LiveMessageUpdater:
    def __init__(
        self,
        chat_id: str,
        message_id: str,
        event_emitter: Optional[Callable[[dict], Awaitable[None]]],
    ):
        self.chat_id = chat_id
        self.message_id = message_id
        self._event_emitter = event_emitter
        self._finished = False

    async def update(self, content: str):
        if self._finished or self._event_emitter is None:
            return

        try:
            await self._event_emitter(
                {
                    "type": "replace",
                    "data": {
                        "chat_id": self.chat_id,
                        "message_id": self.message_id,
                        "content": content,
                    },
                }
            )
        except Exception:
            log.exception(
                "Failed to update background task message %s in chat %s",
                self.message_id,
                self.chat_id,
            )

    async def finish(self):
        self._finished = True


class BackgroundTaskManager:
    def __init__(
        self,
        app_state,
        chat_id: str,
        message_id: str,
        event_emitter: Optional[Callable[[dict], Awaitable[None]]],
    ):
        self._app_state = app_state
        self.chat_id = chat_id
        self.message_id = message_id
        self._event_emitter = event_emitter

    def _get_registry(self) -> set[asyncio.Task]:
        registry = getattr(self._app_state, "tool_background_tasks", None)
        if registry is None:
            registry = set()
            setattr(self._app_state, "tool_background_tasks", registry)
        return registry

    async def _run_worker(self, worker_func):
        updater = LiveMessageUpdater(
            chat_id=self.chat_id,
            message_id=self.message_id,
            event_emitter=self._event_emitter,
        )

        try:
            result = worker_func(updater)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "Background tool task failed for message %s in chat %s",
                self.message_id,
                self.chat_id,
            )
        finally:
            await updater.finish()

    def spawn(self, worker_func):
        task = asyncio.create_task(self._run_worker(worker_func))
        registry = self._get_registry()
        registry.add(task)
        task.add_done_callback(registry.discard)
        return task


def build_background_task_manager(request, extra_params: dict):
    if request is None:
        return None

    existing_task_manager = extra_params.get("__task_manager__") or extra_params.get(
        "_task_manager_"
    )
    if existing_task_manager is not None:
        return existing_task_manager

    metadata = extra_params.get("__metadata__", {})
    chat_id = extra_params.get("__chat_id__") or metadata.get("chat_id")
    message_id = extra_params.get("__message_id__") or metadata.get("message_id")
    event_emitter = extra_params.get("__event_emitter__")

    if not chat_id or not message_id or event_emitter is None:
        return None

    return BackgroundTaskManager(
        app_state=request.app.state,
        chat_id=chat_id,
        message_id=message_id,
        event_emitter=event_emitter,
    )
