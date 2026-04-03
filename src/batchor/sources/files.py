from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from collections.abc import Iterator
from typing import Callable, Generic, TypeVar, cast

from batchor.core.models import BatchItem
from batchor.core.types import JSONObject, JSONValue
from batchor.sources.base import IndexedBatchItem, ResumableItemSource, SourceIdentity

PayloadT = TypeVar("PayloadT")
CsvRow = dict[str, str]
JsonlRow = JSONValue


def _file_fingerprint(path: Path) -> str:
    stat = path.stat()
    payload = f"{path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class CsvItemSource(ResumableItemSource[PayloadT], Generic[PayloadT]):
    def __init__(
        self,
        path: str | Path,
        *,
        item_id_from_row: Callable[[CsvRow], str],
        payload_from_row: Callable[[CsvRow], PayloadT],
        metadata_from_row: Callable[[CsvRow], JSONObject] | None = None,
        encoding: str = "utf-8",
    ) -> None:
        self.path = Path(path)
        self.item_id_from_row = item_id_from_row
        self.payload_from_row = payload_from_row
        self.metadata_from_row = metadata_from_row
        self.encoding = encoding

    def source_identity(self) -> SourceIdentity:
        return SourceIdentity(
            source_kind="csv",
            source_ref=str(self.path.resolve()),
            source_fingerprint=_file_fingerprint(self.path),
        )

    def iter_from(self, item_index: int) -> Iterator[IndexedBatchItem[PayloadT]]:
        if item_index < 0:
            raise ValueError("item_index must be >= 0")
        with self.path.open("r", encoding=self.encoding, newline="") as handle:
            reader = csv.DictReader(handle)
            for current_index, row in enumerate(reader):
                if current_index < item_index:
                    continue
                normalized_row = {str(key): value for key, value in row.items() if key is not None}
                metadata = (
                    self.metadata_from_row(normalized_row)
                    if self.metadata_from_row is not None
                    else {}
                )
                yield IndexedBatchItem(
                    item_index=current_index,
                    item=BatchItem(
                        item_id=self.item_id_from_row(normalized_row),
                        payload=self.payload_from_row(normalized_row),
                        metadata=metadata,
                    ),
                )


class JsonlItemSource(ResumableItemSource[PayloadT], Generic[PayloadT]):
    def __init__(
        self,
        path: str | Path,
        *,
        item_id_from_row: Callable[[JsonlRow], str],
        payload_from_row: Callable[[JsonlRow], PayloadT],
        metadata_from_row: Callable[[JsonlRow], JSONObject] | None = None,
        encoding: str = "utf-8",
    ) -> None:
        self.path = Path(path)
        self.item_id_from_row = item_id_from_row
        self.payload_from_row = payload_from_row
        self.metadata_from_row = metadata_from_row
        self.encoding = encoding

    def source_identity(self) -> SourceIdentity:
        return SourceIdentity(
            source_kind="jsonl",
            source_ref=str(self.path.resolve()),
            source_fingerprint=_file_fingerprint(self.path),
        )

    def iter_from(self, item_index: int) -> Iterator[IndexedBatchItem[PayloadT]]:
        if item_index < 0:
            raise ValueError("item_index must be >= 0")
        current_index = 0
        with self.path.open("r", encoding=self.encoding) as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    parsed = cast(JSONValue, json.loads(line))
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"invalid JSONL record at {self.path}:{line_number}"
                    ) from exc
                metadata = (
                    self.metadata_from_row(parsed)
                    if self.metadata_from_row is not None
                    else {}
                )
                if current_index < item_index:
                    current_index += 1
                    continue
                yield IndexedBatchItem(
                    item_index=current_index,
                    item=BatchItem(
                        item_id=self.item_id_from_row(parsed),
                        payload=self.payload_from_row(parsed),
                        metadata=metadata,
                    ),
                )
                current_index += 1
