from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import io
import sys
import unittest.mock
from typing import Sequence

import click
import pytest
from aiohttp import ClientSession
from prompt_toolkit.application import create_app_session
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput

import aiomonitor.termui.commands
from aiomonitor import Monitor, start_monitor
from aiomonitor.exceptions import MissingTask
from aiomonitor.termui.commands import (
    auto_command_done,
    command_done,
    current_monitor,
    current_stdout,
    custom_help_option,
    monitor_cli,
    print_ok,
)
from aiomonitor.types import (
    FormatItemTypes,
    FormattedLiveTaskInfo,
    FormattedStackItem,
    FormattedTerminatedTaskInfo,
)


@contextlib.contextmanager
def monitor_common():
    def make_baz():
        return "baz"

    locals_ = {"foo": "bar", "make_baz": make_baz}
    # In the tests, we reuse the pytest's event loop as the monitored loop.
    # Because of this, all cross-loop coroutine invocations should use the following
    # pattern in both tests and the monitor/termui implementation:
    # > fut = asyncio.wrap_future(asyncio.run_coroutine_threadsafe(...))
    # > await fut
    test_loop = asyncio.get_running_loop()
    mon = Monitor(test_loop, locals=locals_)
    with mon:
        yield mon


@pytest.fixture
async def monitor(request, event_loop):
    with monitor_common() as monitor_instance:
        yield monitor_instance


def get_task_ids(event_loop):
    return [id(t) for t in asyncio.all_tasks(loop=event_loop)]


class BufferedOutput(DummyOutput):
    def __init__(self) -> None:
        self._buffer = io.StringIO()

    def write(self, data: str) -> None:
        self._buffer.write(data)

    def write_raw(self, data: str) -> None:
        self._buffer.write(data)


async def invoke_command(
    monitor: Monitor,
    args: Sequence[str],
) -> str:
    dummy_stdout = BufferedOutput()
    current_monitor_token = current_monitor.set(monitor)
    current_stdout_token = current_stdout.set(dummy_stdout._buffer)

    async def _ui_create_event() -> asyncio.Event:
        return asyncio.Event()

    fut = asyncio.run_coroutine_threadsafe(_ui_create_event(), monitor._ui_loop)
    command_done_event: asyncio.Event = await asyncio.wrap_future(fut)
    command_done_token = command_done.set(command_done_event)
    try:
        with unittest.mock.patch.object(
            aiomonitor.termui.commands,
            "print_formatted_text",
            functools.partial(
                aiomonitor.termui.commands.print_formatted_text, output=dummy_stdout
            ),
        ):
            ctx = contextvars.copy_context()
            ctx.run(
                monitor_cli.main,
                args,
                prog_name="",
                obj=monitor,
                standalone_mode=False,  # type: ignore
            )
            # If Click raises UsageError before running the command,
            # there will be no one to set command_done_event.
            # In this case, the error is propagated to the upper stack
            # immediately here.
            fut = asyncio.run_coroutine_threadsafe(
                command_done_event.wait(),  # type: ignore
                monitor._ui_loop,
            )
            await asyncio.wrap_future(fut)
    finally:
        command_done.reset(command_done_token)
        current_stdout.reset(current_stdout_token)
        current_monitor.reset(current_monitor_token)
    with contextlib.closing(dummy_stdout._buffer):
        return dummy_stdout._buffer.getvalue()


@pytest.fixture(params=[True, False], ids=["console:True", "console:False"])
def console_enabled(request):
    return request.param


@pytest.mark.asyncio
async def test_ctor(event_loop, console_enabled):
    with Monitor(event_loop, console_enabled=console_enabled):
        await asyncio.sleep(0.01)
    with start_monitor(event_loop, console_enabled=console_enabled) as m:
        await asyncio.sleep(0.01)
    assert m.closed

    m = Monitor(event_loop, console_enabled=console_enabled)
    m.start()
    try:
        await asyncio.sleep(0.01)
    finally:
        m.close()
        m.close()  # make sure call is idempotent
    assert m.closed

    m = Monitor(event_loop, console_enabled=console_enabled)
    m.start()
    with m:
        await asyncio.sleep(0.01)
    assert m.closed

    # make sure that monitor inside async func can exit correctly
    with Monitor(event_loop, console_enabled=console_enabled):
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_basic_monitor(event_loop, monitor: Monitor):
    resp = await invoke_command(monitor, ["help"])
    assert "Commands:" in resp

    with pytest.raises(click.UsageError):
        await invoke_command(monitor, ["xxx"])

    resp = await invoke_command(monitor, ["ps"])
    assert "Task" in resp

    with pytest.raises(click.UsageError):
        await invoke_command(monitor, ["ps", "123"])

    resp = await invoke_command(monitor, ["signal", "name"])
    assert "Unknown signal" in resp

    resp = await invoke_command(monitor, ["stacktrace"])
    assert "self.run_forever()" in resp

    resp = await invoke_command(monitor, ["w", "123"])
    assert "No task 123" in resp

    resp = await invoke_command(monitor, ["where", "123"])
    assert "No task 123" in resp

    with pytest.raises(click.UsageError):
        await invoke_command(monitor, ["c", "123"])

    resp = await invoke_command(monitor, ["cancel", "123"])
    assert "Invalid or non-existent task ID" in resp

    resp = await invoke_command(monitor, ["ca", "123"])
    assert "Invalid or non-existent task ID" in resp


myvar = contextvars.ContextVar("myvar", default=42)


@pytest.mark.asyncio
async def test_monitor_task_factory(event_loop):
    async def do():
        await asyncio.sleep(0)
        myself = asyncio.current_task()
        assert myself is not None
        assert myself.get_name() == "mytask"

    with Monitor(event_loop, console_enabled=False, hook_task_factory=True):
        t = asyncio.create_task(do(), name="mytask")
        await t


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="The context argument of asyncio.create_task() is added in Python 3.11",
)
@pytest.mark.asyncio
async def test_monitor_task_factory_with_context():
    ctx = contextvars.Context()
    # This context is bound at the outermost scope,
    # and inside it the initial value of myvar is kept intact.

    async def do():
        await asyncio.sleep(0)
        assert myvar.get() == 42  # we are referring the outer context
        myself = asyncio.current_task()
        assert myself is not None
        assert myself.get_name() == "mytask"

    myvar.set(99)  # override in the current task's context
    event_loop = asyncio.get_running_loop()
    with Monitor(event_loop, console_enabled=False, hook_task_factory=True):
        t = asyncio.create_task(do(), name="mytask", context=ctx)
        await t
    assert myvar.get() == 99


@pytest.mark.asyncio
async def test_cancel_where_tasks(
    monitor: Monitor,
) -> None:
    async def sleeper():
        await asyncio.sleep(100)  # xxx

    test_loop = monitor._monitored_loop
    t = test_loop.create_task(sleeper())
    t_id = id(t)
    await asyncio.sleep(0.1)

    task_ids = get_task_ids(test_loop)
    assert len(task_ids) > 0
    assert t_id in task_ids
    resp = await invoke_command(monitor, ["where", str(t_id)])
    assert "Task" in resp
    resp = await invoke_command(monitor, ["cancel", str(t_id)])
    assert "Cancelled task" in resp
    assert t.done()


@pytest.mark.asyncio
async def test_monitor_with_console(monitor: Monitor) -> None:
    with create_pipe_input() as pipe_input:
        stdout_buf = BufferedOutput()
        with create_app_session(input=pipe_input, output=stdout_buf):

            async def _interact():
                await asyncio.sleep(0.2)
                try:
                    pipe_input.send_text("await asyncio.sleep(0.1, result=333)\r\n")
                    pipe_input.flush()
                    await asyncio.sleep(0.1)
                    pipe_input.send_text("foo\r\n")
                    pipe_input.flush()
                    await asyncio.sleep(0.4)
                    resp = stdout_buf._buffer.getvalue()
                    assert "This console is running in an asyncio event loop." in resp
                    assert "333" in resp
                    assert "bar" in resp
                finally:
                    pipe_input.send_text("exit()\r\n")

            t = asyncio.create_task(_interact())
            await invoke_command(monitor, ["console"])
            await t
    # Check if we are back to the original shell.
    resp = await invoke_command(monitor, ["help"])
    assert "Commands" in resp


@pytest.mark.asyncio
async def test_custom_monitor_command(monitor: Monitor):
    @monitor_cli.command(name="something")
    @click.argument("arg")
    @custom_help_option
    @auto_command_done
    def do_something(ctx: click.Context, arg: str) -> None:
        print_ok(f"doing something with {arg}")

    resp = await invoke_command(monitor, ["something", "someargument"])
    assert "doing something with someargument" in resp


async def _wait_for_terminated(monitor: Monitor, name: str) -> None:
    for _ in range(50):
        if any(item.name == name for item in monitor._terminated_tasks.values()):
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"{name} was not recorded as terminated")


@pytest.mark.asyncio
async def test_task_snapshots(event_loop):
    async def sleeper():
        await asyncio.Event().wait()

    with Monitor(event_loop, console_enabled=False, hook_task_factory=True) as monitor:
        task = asyncio.create_task(sleeper(), name="snap-sleeper")
        task_id = str(id(task))
        await asyncio.sleep(0.05)

        snapshot_id = monitor.capture_snapshot("before")
        assert snapshot_id == 0
        assert monitor.get_snapshot(0).label == "before"
        running = monitor.format_snapshot_running_task_list(0, "", False)
        assert all(isinstance(item, FormattedLiveTaskInfo) for item in running)
        frozen = next(item for item in running if item.task_id == task_id)
        assert frozen.name == "snap-sleeper"
        same_id = monitor.capture_snapshot("same")
        unchanged = monitor.format_snapshot_diff(0, same_id)
        assert frozen in unchanged.common
        assert frozen not in unchanged.added
        assert frozen not in unchanged.removed
        assert unchanged.common == [
            item
            for item in running
            if item.task_id
            in {
                later.task_id
                for later in monitor.format_snapshot_running_task_list(
                    same_id, "", False
                )
            }
        ]
        terminated = monitor.format_snapshot_terminated_task_list(0, "", False)
        assert all(isinstance(item, FormattedTerminatedTaskInfo) for item in terminated)
        stack = monitor.format_snapshot_running_task_stack(0, task_id)
        assert stack
        assert all(isinstance(item, FormattedStackItem) for item in stack)
        assert any(item.type == FormatItemTypes.HEADER for item in stack)
        with pytest.raises(MissingTask):
            monitor.format_snapshot_running_task_stack(0, "999999")

        resp = await invoke_command(monitor, ["snapshot", "show", "0"])
        assert "snap-sleeper" in resp
        assert "Snapshot 0 (before)" in resp
        resp = await invoke_command(monitor, ["snapshot", "where", "0", task_id])
        assert "snap-sleeper" in resp or "sleeper" in resp
        assert "Snapshot 0 (before)" in resp

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await _wait_for_terminated(monitor, "snap-sleeper")

        resp = await invoke_command(monitor, ["ps"])
        assert "snap-sleeper" not in resp
        resp = await invoke_command(monitor, ["snapshot", "show", "0"])
        assert "snap-sleeper" in resp
        assert "Snapshot 0 (before)" in resp

        second_id = monitor.capture_snapshot()
        assert second_id == 2
        assert monitor.get_snapshot(second_id).label is None
        right = monitor.format_snapshot_running_task_list(second_id, "", False)
        right_ids = {item.task_id for item in right}
        diff = monitor.format_snapshot_diff(0, second_id)
        assert diff.removed == [
            item for item in running if item.task_id not in right_ids
        ]
        assert diff.common == [item for item in running if item.task_id in right_ids]
        assert diff.added == [
            item
            for item in right
            if item.task_id not in {row.task_id for row in running}
        ]
        assert any(item.task_id == task_id for item in diff.removed)
        assert all(isinstance(item, FormattedLiveTaskInfo) for item in diff.removed)

        resp = await invoke_command(monitor, ["snapshot", "save", "--name", "after"])
        assert "Captured snapshot 3 (after)" in resp
        resp = await invoke_command(monitor, ["snap", "save", "--name", "later"])
        assert "Captured snapshot 4 (later)" in resp
        resp = await invoke_command(monitor, ["snapshot", "ls"])
        assert "before" in resp
        assert "after" in resp
        assert "later" in resp
        assert "5 snapshots captured" in resp
        summaries = monitor.list_snapshots()
        assert [item.id for item in summaries] == [0, 1, 2, 3, 4]
        assert [item.name for item in summaries] == [
            "before",
            "same",
            None,
            "after",
            "later",
        ]
        assert summaries[0].running_count >= 1
        assert summaries[0].captured_at > 0
        assert all(item.terminated_count >= 0 for item in summaries)

        with pytest.raises(KeyError):
            monitor.get_snapshot(9)
        with pytest.raises(KeyError):
            monitor.format_snapshot_diff(0, 8)
        with pytest.raises(KeyError):
            monitor.format_snapshot_running_task_list(8, "", False)
        with pytest.raises(KeyError):
            monitor.format_snapshot_terminated_task_list(8, "", False)

        resp = await invoke_command(monitor, ["snapshot", "diff", "0", str(second_id)])
        assert "Snapshot 0 (before)" in resp
        assert "snap-sleeper" in resp
        assert "removed" in resp

        resp = await invoke_command(monitor, ["snapshot", "show", "9"])
        assert "Invalid snapshot id: 9" in resp
        resp = await invoke_command(monitor, ["snapshot", "where", "nope", task_id])
        assert "Invalid snapshot id: nope" in resp
        resp = await invoke_command(monitor, ["snapshot", "diff", "0", "8"])
        assert "Invalid snapshot id: 8" in resp
        resp = await invoke_command(monitor, ["snapshot", "delete", "4"])
        assert "Deleted snapshot 4 (later)" in resp
        assert all(item.id != 4 for item in monitor.list_snapshots())
        with pytest.raises(KeyError):
            monitor.get_snapshot(4)
        resp = await invoke_command(monitor, ["snapshot", "delete", "4"])
        assert "Invalid snapshot id: 4" in resp

        with pytest.raises(click.UsageError):
            await invoke_command(monitor, ["ps", "123"])


@pytest.mark.asyncio
async def test_snapshot_retention(event_loop):
    with Monitor(event_loop, console_enabled=False, max_snapshots=2) as monitor:
        first = monitor.capture_snapshot("oldest")
        second = monitor.capture_snapshot("kept")
        third = monitor.capture_snapshot("newest")
        assert [item.id for item in monitor.list_snapshots()] == [second, third]
        assert [item.name for item in monitor.list_snapshots()] == ["kept", "newest"]
        with pytest.raises(KeyError):
            monitor.get_snapshot(first)
        kept = monitor.get_snapshot(second)
        assert kept.label == "kept"
        assert monitor.list_snapshots()[0].running_count == len(kept.running)
        assert monitor.list_snapshots()[0].terminated_count == len(kept.terminated)

    with start_monitor(event_loop, console_enabled=False, max_snapshots=1) as monitor:
        monitor.capture_snapshot("drop-me")
        kept_id = monitor.capture_snapshot("only")
        listed = monitor.list_snapshots()
        assert len(listed) == 1
        assert listed[0].id == kept_id
        assert listed[0].name == "only"
        with pytest.raises(KeyError):
            monitor.get_snapshot(0)

    with Monitor(event_loop, console_enabled=False) as monitor:
        assert monitor._max_snapshots == 20
        for index in range(21):
            monitor.capture_snapshot(f"n{index}")
        listed = monitor.list_snapshots()
        assert len(listed) == 20
        assert listed[0].id == 1
        assert listed[0].name == "n1"
        assert listed[-1].id == 20
        assert listed[-1].name == "n20"
        with pytest.raises(KeyError):
            monitor.get_snapshot(0)


@pytest.mark.asyncio
async def test_snapshot_web_api(event_loop):
    async def sleeper():
        await asyncio.Event().wait()

    with Monitor(
        event_loop,
        console_enabled=False,
        hook_task_factory=True,
        termui_port=28101,
        webui_port=28102,
        console_port=28103,
    ) as monitor:
        assert monitor.port == 28101
        task = asyncio.create_task(sleeper(), name="web-sleeper")
        task_id = str(id(task))
        await asyncio.sleep(0.05)
        base = "http://127.0.0.1:28102"
        async with ClientSession() as client:
            async with client.get(base + "/") as resp:
                assert resp.status == 200
                page = await resp.text()
                assert "Dashboard" in page
                assert "Snapshots" in page
            async with client.post(base + "/api/live-tasks") as resp:
                assert resp.status == 200
                live = await resp.json()
                assert "tasks" in live

            async with client.post(
                base + "/api/snapshot", data={"name": "web"}
            ) as resp:
                assert resp.status == 200
                created = await resp.json()
                assert created["id"] == 0
            async with client.get(base + "/api/snapshot") as resp:
                assert resp.status == 200
                listed = await resp.json()
                assert listed["snapshots"][0]["id"] == 0
                assert listed["snapshots"][0]["name"] == "web"
                assert listed["snapshots"][0]["running_count"] >= 1
            async with client.get(base + "/api/snapshot/list") as resp:
                assert resp.status == 200
                assert (await resp.json())["snapshots"][0]["name"] == "web"

            async with client.post(
                base + "/api/snapshot/tasks", data={"snapshot_id": "0"}
            ) as resp:
                assert resp.status == 200
                tasks = await resp.json()
                assert any(item["name"] == "web-sleeper" for item in tasks["running"])
                assert "terminated" in tasks
            async with client.post(
                base + "/api/snapshot/trace",
                data={"snapshot_id": "0", "task_id": task_id},
            ) as resp:
                assert resp.status == 200
                trace = await resp.json()
                assert any(item["type"] == "header" for item in trace["trace"])
            async with client.post(
                base + "/api/snapshot/stack",
                data={"snapshot_id": "0", "task_id": task_id},
            ) as resp:
                assert resp.status == 200

            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            async with client.post(base + "/api/snapshot") as resp:
                assert resp.status == 200
                second = await resp.json()
                assert second["id"] == 1
            async with client.post(
                base + "/api/snapshot/diff",
                data={"snapshot_id_1": "0", "snapshot_id_2": "1"},
            ) as resp:
                assert resp.status == 200
                diff = await resp.json()
                assert any(item["task_id"] == task_id for item in diff["removed"])
                assert "added" in diff
                assert "common" in diff

            async with client.delete(
                base + "/api/snapshot", params={"snapshot_id": "0"}
            ) as resp:
                assert resp.status == 200
                assert (await resp.json())["id"] == 0
            async with client.delete(
                base + "/api/snapshot", params={"snapshot_id": "0"}
            ) as resp:
                assert resp.status == 404
            async with client.post(
                base + "/api/snapshot/tasks", data={"snapshot_id": "99"}
            ) as resp:
                assert resp.status == 404
            async with client.post(
                base + "/api/snapshot/diff", data={"snapshot_id_1": "nope"}
            ) as resp:
                assert resp.status == 400

            async with client.get(base + "/snapshots") as resp:
                assert resp.status == 200
                text = await resp.text()
                assert "Snapshots" in text
