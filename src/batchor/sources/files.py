from __future__ import annotations

import csv
import json
from pathlib import Path
from collections.abc import Iterator
from typing import Callable, Generic, TypeVar, cast

from batchor.core.models import BatchItem
from batchor.core.types import JSONObject, JSONValue
from batchor.sources.base import ItemSource

PayloadT = TypeVar("PayloadT")
CsvRow = dict[str, str]
JsonlRow = JSONValue


class CsvItemSource(ItemSource[PayloadT], Generic[PayloadT]):
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

    def __iter__(self) -> Iterator[BatchItem[PayloadT]]:
        with self.path.open("r", encoding=self.encoding, newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                normalized_row = {str(key): value for key, value in row.items() if key is not None}
                metadata = (
                    self.metadata_from_row(normalized_row)
                    if self.metadata_from_row is not None
                    else {}
                )
                yield BatchItem(
                    item_id=self.item_id_from_row(normalized_row),
                    payload=self.payload_from_row(normalized_row),
                    metadata=metadata,
                )


class JsonlItemSource(ItemSource[PayloadT], Generic[PayloadT]):
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

    def __iter__(self) -> Iterator[BatchItem[PayloadT]]:
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
                yield BatchItem(
                    item_id=self.item_id_from_row(parsed),
                    payload=self.payload_from_row(parsed),
                    metadata=metadata,
                )
