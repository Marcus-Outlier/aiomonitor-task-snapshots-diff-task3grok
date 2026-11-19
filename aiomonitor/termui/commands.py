from __future__ import annotations

import asyncio
import functools
import logging
import os
import shlex
import signal
import sys
import textwrap
import time
import traceback
from contextvars import copy_context
from typing import TYPE_CHECKING, List, Optional, Sequence, TextIO, Tuple

import click
from prompt_toolkit import PromptSession
from prompt_toolkit.application.current import get_app_session
from prompt_toolkit.contrib.telnet.server import TelnetConnection
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.shortcuts import print_formatted_text
from terminaltables import AsciiTable

from .. import console
from ..context import command_done, current_monitor, current_stdout
from ..exceptions import MissingTask
from ..types import (
    FormatItemTypes,
    FormattedSnapshotDiff,
    FormattedStackItem,
    TaskSnapshot,
)
from .completion import (
    ClickCompleter,
    complete_signal_names,
    complete_snapshot_id,
    complete_task_id,
    complete_trace_id,
)

if TYPE_CHECKING:
    from ..monitor import Monitor

log = logging.getLogger(__name__)

__all__ = (
    "interact",
    "monitor_cli",
    "auto_command_done",
    "auto_async_command_done",
    "custom_help_option",
)


def _get_current_stdout() -> TextIO:
    stdout = current_stdout.get(None)
    if stdout is None:
        return sys.stdout
    else:
        return stdout


def _get_current_stderr() -> TextIO:
    stdout = current_stdout.get(None)
    if stdout is None:
        return sys.stderr
    else:
        return stdout


click.utils._default_text_stdout = _get_current_stdout
click.utils._default_text_stderr = _get_current_stderr


def print_ok(msg: str) -> None:
    print_formatted_text(
        FormattedText([
            ("ansibrightgreen", "✓ "),
            ("", msg),
        ])
    )


def print_fail(msg: str) -> None:
    print_formatted_text(
        FormattedText([
            ("ansibrightred", "✗ "),
            ("", msg),
        ])
    )


def _write_stack(
    stdout: TextIO, formatted_stack_list: Sequence[FormattedStackItem]
) -> None:
    if not formatted_stack_list:
        stdout.write("No stack available\n")
        stdout.flush()
        return
    for item_type, item_text in formatted_stack_list:
        if item_type == FormatItemTypes.HEADER:
            stdout.write("\n")
            print_formatted_text(
                FormattedText([
                    ("ansiwhite", item_text),
                ])
            )
        else:
            stdout.write(textwrap.indent(item_text.strip("\n"), "  "))
            stdout.write("\n")
    stdout.flush()


async def interact(self: Monitor, connection: TelnetConnection) -> None:
    """
    The interactive loop for each telnet client connection.
    """
    await asyncio.sleep(0.3)  # wait until telnet negotiation is done
    tasknum = len(asyncio.all_tasks(loop=self._monitored_loop))  # TODO: refactor
    s = "" if tasknum == 1 else "s"
    intro = (
        f"\nAsyncio Monitor: {tasknum} task{s} running\n"
        f"Type help for available commands\n"
    )
    print(intro, file=connection.stdout)

    # Override the Click's stdout/stderr reference cache functions
    # to let them use the correct stdout handler.
    current_monitor_token = current_monitor.set(self)
    current_stdout_token = current_stdout.set(connection.stdout)
    # NOTE: prompt_toolkit's all internal console output automatically uses
    #       an internal contextvar to keep the stdout consistent with the
    #       current telnet connection.
    prompt_session: PromptSession[str] = PromptSession(
        completer=ClickCompleter(monitor_cli),
        complete_while_typing=False,
    )
    lastcmd = "noop"
    style_prompt = "#5fd7ff bold"
    try:
        while True:
            try:
                user_input = (
                    await prompt_session.prompt_async(
                        FormattedText([
                            (style_prompt, self.prompt),
                        ])
                    )
                ).strip()
            except KeyboardInterrupt:
                print_fail("To terminate, press Ctrl+D or type 'exit'.")
            except (EOFError, asyncio.CancelledError):
                return
            except Exception:
                print_fail(traceback.format_exc())
            else:
                command_done_event = asyncio.Event()
                command_done_token = command_done.set(command_done_event)
                try:
                    if not user_input and lastcmd is not None:
                        user_input = lastcmd
                    args = shlex.split(user_input)
                    term_size = prompt_session.output.get_size()
                    ctx = copy_context()
                    ctx.run(
                        monitor_cli.main,
                        args,
                        prog_name="",
                        obj=self,
                        standalone_mode=False,  # type: ignore
                        max_content_width=term_size.columns,
                    )
                    await command_done_event.wait()
                    if args[0] == "console":
                        lastcmd = "noop"
                    else:
                        lastcmd = user_input
                except (click.BadParameter, click.UsageError) as e:
                    print_fail(str(e))
                except asyncio.CancelledError:
                    return
                except Exception:
                    print_fail(traceback.format_exc())
                finally:
                    command_done.reset(command_done_token)
    finally:
        current_stdout.reset(current_stdout_token)
        current_monitor.reset(current_monitor_token)


class AliasGroupMixin(click.Group):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._commands = {}
        self._aliases = {}

    def command(self, *args, **kwargs):
        aliases = kwargs.pop("aliases", [])
        decorator = super().command(*args, **kwargs)

        def _decorator(f):
            cmd = decorator(click.pass_context(f))
            if aliases:
                self._commands[cmd.name] = aliases
                for alias in aliases:
                    self._aliases[alias] = cmd.name
            return cmd

        return _decorator

    def group(self, *args, **kwargs):
        aliases = kwargs.pop("aliases", [])
        # keep the same class type
        kwargs["cls"] = type(self)
        decorator = super().group(*args, **kwargs)
        if not aliases:
            return decorator

        def _decorator(f):
            cmd = decorator(f)
            if aliases:
                self._commands[cmd.name] = aliases
                for alias in aliases:
                    self._aliases[alias] = cmd.name
            return cmd

        return _decorator

    def get_command(self, ctx, cmd_name):
        if cmd_name in self._aliases:
            cmd_name = self._aliases[cmd_name]
        command = super().get_command(ctx, cmd_name)
        if command:
            return command

    def format_commands(self, ctx, formatter):
        commands = []
        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            # What is this, the tool lied about a command. Ignore it
            if cmd is None:
                continue
            if cmd.hidden:
                continue
            if subcommand in self._commands:
                aliases = ",".join(sorted(self._commands[subcommand]))
                subcommand = "{0} ({1})".format(subcommand, aliases)
            commands.append((subcommand, cmd))

        # allow for 3 times the default spacing
        if len(commands):
            limit = formatter.width - 6 - max(len(cmd[0]) for cmd in commands)
            rows = []
            for subcommand, cmd in commands:
                help = cmd.get_short_help_str(limit)
                rows.append((subcommand, help))
            if rows:
                with formatter.section("Commands"):
                    formatter.write_dl(rows)


@click.group(cls=AliasGroupMixin, add_help_option=False)
def monitor_cli():
    """
    To see the usage of each command, run them with "--help" option.
    """
    pass


def auto_command_done(cmdfunc):
    @functools.wraps(cmdfunc)
    def _inner(ctx: click.Context, *args, **kwargs):
        command_done_event = command_done.get()
        try:
            return cmdfunc(ctx, *args, **kwargs)
        finally:
            command_done_event.set()

    return _inner


def auto_async_command_done(cmdfunc):
    @functools.wraps(cmdfunc)
    async def _inner(ctx: click.Context, *args, **kwargs):
        command_done_event = command_done.get()
        command_done_event.clear()
        try:
            return await cmdfunc(ctx, *args, **kwargs)
        finally:
            command_done_event.set()

    return _inner


def custom_help_option(cmdfunc):
    """
    A custom help option to ensure setting `command_done_event`.
    """

    @auto_command_done
    def show_help(ctx: click.Context, param: click.Parameter, value: bool) -> None:
        if not value:
            return
        click.echo(ctx.get_help(), color=ctx.color)
        ctx.exit()

    return click.option(
        "--help",
        is_flag=True,
        expose_value=False,
        is_eager=True,
        callback=show_help,
        help="Show the help message",
    )(cmdfunc)


@monitor_cli.command(name="noop", hidden=True)
@auto_command_done
def do_noop(ctx: click.Context) -> None:
    pass


@monitor_cli.command(name="help", aliases=["?", "h"])
@custom_help_option
@auto_command_done
def do_help(ctx: click.Context) -> None:
    """Show the list of commands"""
    click.echo(monitor_cli.get_help(ctx))


@monitor_cli.command(name="signal")
@click.argument("signame", type=str, shell_complete=complete_signal_names)
@custom_help_option
@auto_command_done
def do_signal(ctx: click.Context, signame: str) -> None:
    """Send a Unix signal"""
    if hasattr(signal, signame):
        os.kill(os.getpid(), getattr(signal, signame))
        print_ok(f"Sent signal to {signame} PID {os.getpid()}")
    else:
        print_fail(f"Unknown signal {signame}")


@monitor_cli.command(name="stacktrace", aliases=["st", "stack"])
@custom_help_option
@auto_command_done
def do_stacktrace(ctx: click.Context) -> None:
    """Print a stack trace from the event loop thread"""
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    tid = self._event_loop_thread_id
    assert tid is not None
    frame = sys._current_frames()[tid]
    traceback.print_stack(frame, file=stdout)


@monitor_cli.command(name="cancel", aliases=["ca"])
@click.argument("taskid", shell_complete=complete_task_id)
@custom_help_option
def do_cancel(ctx: click.Context, taskid: str) -> None:
    """Cancel an indicated task"""
    self: Monitor = ctx.obj

    @auto_async_command_done
    async def _do_cancel(ctx: click.Context) -> None:
        try:
            await self.cancel_monitored_task(taskid)
            print_ok(f"Cancelled task {taskid}")
        except ValueError as e:
            print_fail(repr(e))

    task = self._ui_loop.create_task(_do_cancel(ctx))
    self._termui_tasks.add(task)


@monitor_cli.command(name="exit", aliases=["q", "quit"])
@custom_help_option
@auto_command_done
def do_exit(ctx: click.Context) -> None:
    """Leave the monitor client session"""
    raise asyncio.CancelledError("exit by user")


@monitor_cli.command(name="console")
@custom_help_option
def do_console(ctx: click.Context) -> None:
    """Switch to async Python REPL"""
    self: Monitor = ctx.obj
    if not self._console_enabled:
        print_fail("Python console is disabled for this session!")
        return

    @auto_async_command_done
    async def _console(ctx: click.Context) -> None:
        log.info("Starting aioconsole at %s:%d", self._host, self._console_port)
        app_session = get_app_session()
        server = await console.start(
            self._host,
            self._console_port,
            self.console_locals,
            self._monitored_loop,
        )
        try:
            await console.proxy(
                app_session.input,
                app_session.output,
                self._host,
                self._console_port,
            )
        except asyncio.CancelledError:
            raise
        finally:
            await console.close(server, self._monitored_loop)
            log.info("Terminated aioconsole at %s:%d", self._host, self._console_port)
            print_ok("The console session is closed.")

    # Since we are already inside the UI's event loop,
    # spawn the async command function as a new task and let it
    # set `command_done_event` internally.
    task = self._ui_loop.create_task(_console(ctx))
    self._termui_tasks.add(task)


@monitor_cli.command(name="ps", aliases=["p"])
@click.option("-f", "--filter", "filter_", help="filter by coroutine or task name")
@click.option("-p", "--persistent", is_flag=True, help="show only persistent tasks")
@click.option(
    "-s",
    "--snapshot",
    "snapshot_id",
    type=int,
    default=None,
    shell_complete=complete_snapshot_id,
    help="show a captured snapshot instead of the live tasks",
)
@custom_help_option
@auto_command_done
def do_ps(
    ctx: click.Context,
    filter_: str,
    persistent: bool,
    snapshot_id: Optional[int],
) -> None:
    """Show task table"""
    headers = (
        "Task ID",
        "State",
        "Name",
        "Coroutine",
        "Created Location",
        "Since",
    )
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    table_data: List[Tuple[str, str, str, str, str, str]] = [headers]
    if snapshot_id is not None:
        try:
            tasks = list(
                self.format_snapshot_running_task_list(snapshot_id, filter_, persistent)
            )
        except KeyError:
            print_fail(f"No snapshot {snapshot_id}")
            return
    else:
        tasks = list(self.format_running_task_list(filter_, persistent))
    for task in tasks:
        table_data.append((
            task.task_id,
            task.state,
            task.name,
            task.coro,
            task.created_location,
            task.since,
        ))
    table = AsciiTable(table_data)
    table.inner_row_border = False
    table.inner_column_border = False
    if snapshot_id is not None:
        stdout.write(f"{len(tasks)} tasks running (snapshot {snapshot_id})\n")
    elif filter_ or persistent:
        stdout.write(
            f"{len(tasks)} tasks running (showing {len(table_data) - 1} tasks)\n"
        )
    else:
        stdout.write(f"{len(tasks)} tasks running\n")
    stdout.write(table.table)
    stdout.write("\n")
    stdout.flush()


@monitor_cli.command(name="ps-terminated", aliases=["pt", "pst"])
@click.option("-f", "--filter", "filter_", help="filter by coroutine or task name")
@click.option("-p", "--persistent", is_flag=True, help="show only persistent tasks")
@click.option(
    "-s",
    "--snapshot",
    "snapshot_id",
    type=int,
    default=None,
    shell_complete=complete_snapshot_id,
    help="show a captured snapshot instead of the live termination log",
)
@custom_help_option
@auto_command_done
def do_ps_terminated(
    ctx: click.Context,
    filter_: str,
    persistent: bool,
    snapshot_id: Optional[int],
) -> None:
    """List recently terminated/cancelled tasks"""
    headers = (
        "Trace ID",
        "Name",
        "Coro",
        "Since Started",
        "Since Terminated",
    )
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    table_data: List[Tuple[str, str, str, str, str]] = [headers]
    if snapshot_id is not None:
        try:
            tasks = list(
                self.format_snapshot_terminated_task_list(
                    snapshot_id, filter_, persistent
                )
            )
        except KeyError:
            print_fail(f"No snapshot {snapshot_id}")
            return
    else:
        tasks = list(self.format_terminated_task_list(filter_, persistent))
    for task in tasks:
        table_data.append((
            task.task_id,
            task.name,
            task.coro,
            task.started_since,
            task.terminated_since,
        ))
    table = AsciiTable(table_data)
    table.inner_row_border = False
    table.inner_column_border = False
    if snapshot_id is not None:
        stdout.write(f"{len(tasks)} tasks terminated (snapshot {snapshot_id})\n")
    elif filter_ or persistent:
        stdout.write(
            f"{len(tasks)} tasks terminated (showing {len(table_data) - 1} tasks)\n"
        )
    else:
        stdout.write(f"{len(tasks)} tasks terminated (old ones may be stripped)\n")
    stdout.write(table.table)
    stdout.write("\n")
    stdout.flush()


@monitor_cli.command(name="where", aliases=["w"])
@click.argument("taskid", shell_complete=complete_task_id)
@click.option(
    "-s",
    "--snapshot",
    "snapshot_id",
    type=int,
    default=None,
    shell_complete=complete_snapshot_id,
    help="read the stack from a captured snapshot",
)
@custom_help_option
@auto_command_done
def do_where(
    ctx: click.Context,
    taskid: str,
    snapshot_id: Optional[int],
) -> None:
    """Show stack frames and the task creation chain of a task"""
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    if snapshot_id is not None:
        try:
            formatted_stack_list = self.format_snapshot_running_task_stack(
                snapshot_id, taskid
            )
        except KeyError:
            print_fail(f"No snapshot {snapshot_id}")
            return
        except MissingTask as e:
            print_fail(f"No task {e.task_id}")
            return
        _write_stack(stdout, formatted_stack_list)
        return
    try:
        formatted_stack_list = self.format_running_task_stack(taskid)
    except MissingTask as e:
        print_fail(f"No task {e.task_id}")
        return
    _write_stack(stdout, formatted_stack_list)


@monitor_cli.command(name="where-terminated", aliases=["wt"])
@click.argument("trace_id", shell_complete=complete_trace_id)
@click.option(
    "-s",
    "--snapshot",
    "snapshot_id",
    type=int,
    default=None,
    shell_complete=complete_snapshot_id,
    help="read the stack from a captured snapshot",
)
@custom_help_option
@auto_command_done
def do_where_terminated(
    ctx: click.Context,
    trace_id: str,
    snapshot_id: Optional[int],
) -> None:
    """Show stack frames and the termination/cancellation chain of a task"""
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    if snapshot_id is not None:
        try:
            formatted_stack_list = self.format_snapshot_terminated_task_stack(
                snapshot_id, trace_id
            )
        except KeyError:
            print_fail(f"No snapshot {snapshot_id}")
            return
        except MissingTask as e:
            print_fail(f"No task {e.task_id}")
            return
        _write_stack(stdout, formatted_stack_list)
        return
    try:
        formatted_stack_list = self.format_terminated_task_stack(trace_id)
    except MissingTask as e:
        print_fail(f"No task {e.task_id}")
        return
    _write_stack(stdout, formatted_stack_list)


def _format_captured_at(captured_at: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(captured_at))


def _snapshot_label(snapshot: TaskSnapshot) -> str:
    return snapshot.label if snapshot.label else "-"


def _write_snapshot_diff(
    stdout: TextIO,
    diff: FormattedSnapshotDiff,
    left: TaskSnapshot,
    right: TaskSnapshot,
) -> None:
    stdout.write(
        f"Snapshot {left.id} ({_snapshot_label(left)})"
        f" -> snapshot {right.id} ({_snapshot_label(right)})\n"
    )
    stdout.write(
        f"{len(diff.added)} added, {len(diff.removed)} removed,"
        f" {len(diff.common)} common\n"
    )
    for task in diff.added:
        stdout.write(f"+ {task.task_id} {task.state} {task.name} {task.coro}\n")
    for task in diff.removed:
        stdout.write(f"- {task.task_id} {task.state} {task.name} {task.coro}\n")
    stdout.flush()


@monitor_cli.command(name="snapshot", aliases=["snap"])
@click.option(
    "-l",
    "--label",
    "label",
    default=None,
    help="optional label stored with the snapshot",
)
@click.argument("label_arg", required=False, metavar="LABEL")
@custom_help_option
@auto_command_done
def do_snapshot(
    ctx: click.Context,
    label: Optional[str],
    label_arg: Optional[str],
) -> None:
    """Capture running and terminated tasks"""
    self: Monitor = ctx.obj
    chosen = label if label is not None else label_arg
    snapshot_id = self.capture_snapshot(chosen)
    if chosen and chosen.strip():
        print_ok(f"Captured snapshot {snapshot_id} ({chosen.strip()})")
    else:
        print_ok(f"Captured snapshot {snapshot_id}")


@monitor_cli.command(name="snapshots", aliases=["snaps"])
@custom_help_option
@auto_command_done
def do_snapshots(ctx: click.Context) -> None:
    """List captured snapshots"""
    headers = ("ID", "Name", "Captured", "Running", "Terminated")
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    snapshots = self.list_snapshots()
    table_data: List[Tuple[str, str, str, str, str]] = [headers]
    for snapshot in snapshots:
        table_data.append((
            str(snapshot.id),
            snapshot.name if snapshot.name else "-",
            _format_captured_at(snapshot.captured_at),
            str(snapshot.running_count),
            str(snapshot.terminated_count),
        ))
    table = AsciiTable(table_data)
    table.inner_row_border = False
    table.inner_column_border = False
    stdout.write(f"{len(snapshots)} snapshots captured\n")
    stdout.write(table.table)
    stdout.write("\n")
    stdout.flush()


@monitor_cli.command(name="snapshot-diff", aliases=["diff", "sd"])
@click.argument("left_id", type=int, shell_complete=complete_snapshot_id)
@click.argument("right_id", type=int, shell_complete=complete_snapshot_id)
@custom_help_option
@auto_command_done
def do_snapshot_diff(ctx: click.Context, left_id: int, right_id: int) -> None:
    """Compare two captured snapshots"""
    self: Monitor = ctx.obj
    stdout = _get_current_stdout()
    try:
        diff = self.format_snapshot_diff(left_id, right_id)
    except KeyError as e:
        missing = e.args[0] if e.args else left_id
        print_fail(f"No snapshot {missing}")
        return
    left = self.get_snapshot(left_id)
    right = self.get_snapshot(right_id)
    _write_snapshot_diff(stdout, diff, left, right)
