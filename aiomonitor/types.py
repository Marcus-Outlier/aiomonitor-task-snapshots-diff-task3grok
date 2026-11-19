from __future__ import annotations

import sys
import traceback
from dataclasses import dataclass
from typing import List, NamedTuple, Optional

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


@dataclass
class FormattedLiveTaskInfo:
    task_id: str
    state: str
    name: str
    coro: str
    created_location: str
    since: str


@dataclass
class FormattedTerminatedTaskInfo:
    task_id: str
    name: str
    coro: str
    started_since: str
    terminated_since: str


class FormatItemTypes(StrEnum):
    HEADER = "header"
    CONTENT = "content"


class FormattedStackItem(NamedTuple):
    type: FormatItemTypes
    content: str


@dataclass
class TerminatedTaskInfo:
    id: str
    name: str
    coro: str
    started_at: float
    terminated_at: float
    cancelled: bool
    termination_stack: Optional[List[traceback.FrameSummary]] = None
    canceller_stack: Optional[List[traceback.FrameSummary]] = None
    exc_repr: Optional[str] = None
    persistent: bool = False


@dataclass
class CancellationChain:
    target_id: str
    canceller_id: str
    canceller_stack: Optional[List[traceback.FrameSummary]] = None


@dataclass(frozen=True)
class LiveTaskSnapshot:
    """A running task frozen at the moment a snapshot was taken."""

    task_id: str
    state: str
    name: str
    coro: str
    created_location: str
    since: str
    persistent: bool
    stack: tuple[FormattedStackItem, ...]


@dataclass(frozen=True)
class TerminatedTaskSnapshot:
    """A terminated task frozen at the moment a snapshot was taken."""

    task_id: str
    name: str
    coro: str
    started_since: str
    terminated_since: str
    persistent: bool
    cancelled: bool
    stack: tuple[FormattedStackItem, ...]


@dataclass(frozen=True)
class TaskSnapshot:
    """Point-in-time copy of the running and terminated task tables."""

    id: int
    label: Optional[str]
    captured_at: float
    running: tuple[LiveTaskSnapshot, ...]
    terminated: tuple[TerminatedTaskSnapshot, ...]


@dataclass(frozen=True)
class TaskSnapshotSummary:
    """Short description of one retained snapshot.

    ``name`` is the optional label. ``captured_at`` is the Unix time the
    snapshot was taken. The counts are how many running and terminated tasks
    were frozen.
    """

    id: int
    name: Optional[str]
    captured_at: float
    running_count: int
    terminated_count: int


@dataclass
class FormattedSnapshotDiff:
    """Running tasks compared by task id.

    Lists keep the order of the formatted task tables. ``added`` tasks are
    only in the later snapshot. ``removed`` tasks are only in the earlier
    one. ``common`` tasks are in both, using the earlier snapshot's rows.
    A different elapsed-time string does not take a task out of ``common``.
    """

    added: List[FormattedLiveTaskInfo]
    removed: List[FormattedLiveTaskInfo]
    common: List[FormattedLiveTaskInfo]
