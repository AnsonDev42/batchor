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
    source_kind: str
    source_ref: str
    source_fingerprint: str


@dataclass(frozen=True)
class IndexedBatchItem(Generic[PayloadT]):
    item_index: int
    item: BatchItem[PayloadT]


@dataclass(frozen=True)
class CheckpointedBatchItem(Generic[PayloadT]):
    next_checkpoint: JSONValue
    item: BatchItem[PayloadT]


class ItemSource(ABC, Generic[PayloadT]):
    @abstractmethod
    def __iter__(self) -> Iterator[BatchItem[PayloadT]]:
        """Yield batch items without materializing the full source in memory."""


class CheckpointedItemSource(ItemSource[PayloadT], Generic[PayloadT]):
    @abstractmethod
    def source_identity(self) -> SourceIdentity:
        """Return a stable identity used to validate resume compatibility."""

    @abstractmethod
    def initial_checkpoint(self) -> JSONValue:
        """Return the initial checkpoint for iteration."""

    @abstractmethod
    def iter_from_checkpoint(
        self,
        checkpoint: JSONValue,
    ) -> Iterator[CheckpointedBatchItem[PayloadT]]:
        """Yield items plus the next opaque checkpoint after each durable item."""

    def __iter__(self) -> Iterator[BatchItem[PayloadT]]:
        for checkpointed_item in self.iter_from_checkpoint(self.initial_checkpoint()):
            yield checkpointed_item.item


class ResumableItemSource(CheckpointedItemSource[PayloadT], Generic[PayloadT]):
    @abstractmethod
    def iter_from(self, item_index: int) -> Iterator[IndexedBatchItem[PayloadT]]:
        """Yield items beginning at the given 0-based source item index."""

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
