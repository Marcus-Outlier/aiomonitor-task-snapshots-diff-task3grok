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
class TaskSnapshotDiff:
    """Difference between two task snapshots, from left to right."""

    left_id: int
    right_id: int
    running_added: tuple[LiveTaskSnapshot, ...]
    running_removed: tuple[LiveTaskSnapshot, ...]
    running_changed: tuple[tuple[LiveTaskSnapshot, LiveTaskSnapshot], ...]
    terminated_added: tuple[TerminatedTaskSnapshot, ...]
    terminated_removed: tuple[TerminatedTaskSnapshot, ...]
    terminated_changed: tuple[
        tuple[TerminatedTaskSnapshot, TerminatedTaskSnapshot], ...
    ]
