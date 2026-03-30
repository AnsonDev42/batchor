from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Generic, TypeVar

from batchor.core.models import BatchItem

PayloadT = TypeVar("PayloadT")


class ItemSource(ABC, Generic[PayloadT]):
    @abstractmethod
    def __iter__(self) -> Iterator[BatchItem[PayloadT]]:
        """Yield batch items without materializing the full source in memory."""
