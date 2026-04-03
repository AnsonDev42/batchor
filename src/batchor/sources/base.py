from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Generic, TypeVar

from batchor.core.models import BatchItem

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


class ItemSource(ABC, Generic[PayloadT]):
    @abstractmethod
    def __iter__(self) -> Iterator[BatchItem[PayloadT]]:
        """Yield batch items without materializing the full source in memory."""


class ResumableItemSource(ItemSource[PayloadT], Generic[PayloadT]):
    @abstractmethod
    def source_identity(self) -> SourceIdentity:
        """Return a stable identity used to validate resume compatibility."""

    @abstractmethod
    def iter_from(self, item_index: int) -> Iterator[IndexedBatchItem[PayloadT]]:
        """Yield items beginning at the given 0-based source item index."""

    def __iter__(self) -> Iterator[BatchItem[PayloadT]]:
        for indexed_item in self.iter_from(0):
            yield indexed_item.item
