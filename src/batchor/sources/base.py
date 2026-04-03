"""Abstract base classes for item source adapters.

Item sources are responsible for streaming :class:`~batchor.BatchItem` objects
to the runner.  The hierarchy provides three levels of capability:

* :class:`ItemSource` — a plain iterable (no resume support).
* :class:`CheckpointedItemSource` — supports durable checkpoints for
  resuming mid-stream after a process restart.
* :class:`ResumableItemSource` — a convenience sub-class of
  :class:`CheckpointedItemSource` that uses a monotonic integer index as the
  checkpoint (suitable for ordered, seekable sources like CSV and JSONL files).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Generic, TypeVar

from batchor.core.models import BatchItem
from batchor.core.types import JSONValue

PayloadT = TypeVar("PayloadT")


@dataclass(frozen=True)
class SourceIdentity:
    """Stable identity for a checkpointed source, used to validate resumability.

    When a run is resumed the runner compares the stored identity against the
    current source's identity to detect mismatches (e.g. the source file was
    replaced).

    Attributes:
        source_kind: Short identifier for the source type (e.g. ``"csv"``).
        source_ref: Human-readable reference to the source (e.g. absolute
            file path).
        source_fingerprint: Content fingerprint (e.g. SHA-256 of size +
            mtime) used to detect source mutations.
    """

    source_kind: str
    source_ref: str
    source_fingerprint: str


@dataclass(frozen=True)
class IndexedBatchItem(Generic[PayloadT]):
    """A :class:`~batchor.BatchItem` paired with its 0-based source index.

    Attributes:
        item_index: 0-based position of this item within the source.
        item: The batch item.
    """

    item_index: int
    item: BatchItem[PayloadT]


@dataclass(frozen=True)
class CheckpointedBatchItem(Generic[PayloadT]):
    """A :class:`~batchor.BatchItem` paired with an opaque resume checkpoint.

    The checkpoint is an opaque :data:`~batchor.core.types.JSONValue` that,
    when passed back to
    :meth:`~batchor.CheckpointedItemSource.iter_from_checkpoint`, resumes
    iteration at the item *after* this one.

    Attributes:
        next_checkpoint: Checkpoint to persist after processing this item.
        item: The batch item.
    """

    next_checkpoint: JSONValue
    item: BatchItem[PayloadT]


class ItemSource(ABC, Generic[PayloadT]):
    """Abstract iterable source of :class:`~batchor.BatchItem` objects.

    Implement this class when your source does not need resume support.  For
    durable checkpointing use :class:`CheckpointedItemSource` instead.
    """

    @abstractmethod
    def __iter__(self) -> Iterator[BatchItem[PayloadT]]:
        """Yield batch items without materializing the full source in memory."""


class CheckpointedItemSource(ItemSource[PayloadT], Generic[PayloadT]):
    """Item source with durable checkpoint support for mid-stream resumption.

    The runner persists the checkpoint returned by each
    :class:`CheckpointedBatchItem` to state.  On resume it calls
    :meth:`iter_from_checkpoint` with the last persisted checkpoint so
    iteration continues exactly where it left off.
    """

    @abstractmethod
    def source_identity(self) -> SourceIdentity:
        """Return a stable identity used to validate resume compatibility."""

    @abstractmethod
    def initial_checkpoint(self) -> JSONValue:
        """Return the checkpoint that represents the start of the source.

        Returns:
            An opaque :data:`~batchor.core.types.JSONValue` accepted by
            :meth:`iter_from_checkpoint` to begin iteration from the first item.
        """

    @abstractmethod
    def iter_from_checkpoint(
        self,
        checkpoint: JSONValue,
    ) -> Iterator[CheckpointedBatchItem[PayloadT]]:
        """Yield items plus the next opaque checkpoint after each durable item.

        Args:
            checkpoint: The checkpoint at which to resume.  Must be a value
                previously returned by a :class:`CheckpointedBatchItem`.

        Yields:
            :class:`CheckpointedBatchItem` instances in source order.
        """

    def __iter__(self) -> Iterator[BatchItem[PayloadT]]:
        for checkpointed_item in self.iter_from_checkpoint(self.initial_checkpoint()):
            yield checkpointed_item.item


class ResumableItemSource(CheckpointedItemSource[PayloadT], Generic[PayloadT]):
    """Checkpointed source where the checkpoint is a 0-based integer index.

    Suitable for ordered seekable sources (CSV, JSONL) where iteration can
    efficiently skip ahead to a given row index.  The checkpoint is simply the
    index of the *next* item to yield.
    """

    @abstractmethod
    def iter_from(self, item_index: int) -> Iterator[IndexedBatchItem[PayloadT]]:
        """Yield items beginning at the given 0-based source item index.

        Args:
            item_index: 0-based index of the first item to yield.  Items
                before this index are skipped.

        Yields:
            :class:`IndexedBatchItem` instances starting from *item_index*.
        """

    def initial_checkpoint(self) -> JSONValue:
        return 0

    def iter_from_checkpoint(
        self,
        checkpoint: JSONValue,
    ) -> Iterator[CheckpointedBatchItem[PayloadT]]:
        if not isinstance(checkpoint, int):
            raise TypeError("resumable source checkpoint must be an int")
        for indexed_item in self.iter_from(checkpoint):
            yield CheckpointedBatchItem(
                next_checkpoint=indexed_item.item_index + 1,
                item=indexed_item.item,
            )

    def __iter__(self) -> Iterator[BatchItem[PayloadT]]:
        for indexed_item in self.iter_from(0):
            yield indexed_item.item
