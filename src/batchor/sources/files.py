from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from collections.abc import Iterator
from typing import Any, Callable, Generic, TypeVar, cast

from batchor.core.models import BatchItem
from batchor.core.types import JSONObject, JSONValue
from batchor.sources.base import (
    CheckpointedBatchItem,
    CheckpointedItemSource,
    IndexedBatchItem,
    ResumableItemSource,
    SourceIdentity,
)

PayloadT = TypeVar("PayloadT")
CsvRow = dict[str, str]
JsonlRow = JSONValue
ParquetRow = dict[str, object]


def _file_fingerprint(path: Path) -> str:
    stat = path.stat()
    payload = f"{path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _lineage_metadata(
    metadata: JSONObject,
    *,
    source_ref: str,
    source_item_index: int,
    source_primary_key: str | None = None,
    partition_id: str | None = None,
) -> JSONObject:
    normalized = dict(metadata)
    lineage = normalized.get("batchor_lineage", {})
    if lineage is None:
        lineage = {}
    if not isinstance(lineage, dict):
        raise TypeError("batchor_lineage metadata must be a JSON object")
    lineage_payload: dict[str, object] = dict(lineage)
    lineage_payload.setdefault("source_ref", source_ref)
    lineage_payload.setdefault("source_item_index", source_item_index)
    if source_primary_key is not None:
        lineage_payload.setdefault("source_primary_key", source_primary_key)
    if partition_id is not None:
        lineage_payload.setdefault("partition_id", partition_id)
    normalized["batchor_lineage"] = cast(JSONObject, lineage_payload)
    return normalized


def _parquet_checkpoint_payload(
    *,
    row_group_index: int,
    row_index_within_group: int,
) -> JSONObject:
    return {
        "row_group_index": row_group_index,
        "row_index_within_group": row_index_within_group,
    }


def _normalized_parquet_row(payload_by_column: dict[str, list[Any]], row_index: int) -> ParquetRow:
    return {
        column: values[row_index]
        for column, values in payload_by_column.items()
    }


class CsvItemSource(ResumableItemSource[PayloadT], Generic[PayloadT]):
    """Stream `BatchItem` values from a CSV file with durable resume support."""

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
                row_id = self.item_id_from_row(normalized_row)
                yield IndexedBatchItem(
                    item_index=current_index,
                    item=BatchItem(
                        item_id=row_id,
                        payload=self.payload_from_row(normalized_row),
                        metadata=_lineage_metadata(
                            metadata,
                            source_ref=str(self.path.resolve()),
                            source_item_index=current_index,
                            source_primary_key=row_id,
                        ),
                    ),
                )


class JsonlItemSource(ResumableItemSource[PayloadT], Generic[PayloadT]):
    """Stream `BatchItem` values from a JSONL file with durable resume support."""

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
                if current_index < item_index:
                    current_index += 1
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
                row_id = self.item_id_from_row(parsed)
                yield IndexedBatchItem(
                    item_index=current_index,
                    item=BatchItem(
                        item_id=row_id,
                        payload=self.payload_from_row(parsed),
                        metadata=_lineage_metadata(
                            metadata,
                            source_ref=str(self.path.resolve()),
                            source_item_index=current_index,
                            source_primary_key=row_id,
                        ),
                    ),
                )
                current_index += 1


class ParquetItemSource(CheckpointedItemSource[PayloadT], Generic[PayloadT]):
    def __init__(
        self,
        path: str | Path,
        *,
        item_id_from_row: Callable[[ParquetRow], str],
        payload_from_row: Callable[[ParquetRow], PayloadT],
        metadata_from_row: Callable[[ParquetRow], JSONObject] | None = None,
        columns: list[str] | None = None,
    ) -> None:
        self.path = Path(path)
        self.item_id_from_row = item_id_from_row
        self.payload_from_row = payload_from_row
        self.metadata_from_row = metadata_from_row
        self.columns = columns

    def source_identity(self) -> SourceIdentity:
        return SourceIdentity(
            source_kind="parquet",
            source_ref=str(self.path.resolve()),
            source_fingerprint=_file_fingerprint(self.path),
        )

    def initial_checkpoint(self) -> JSONValue:
        return _parquet_checkpoint_payload(
            row_group_index=0,
            row_index_within_group=0,
        )

    def iter_from_checkpoint(
        self,
        checkpoint: JSONValue,
    ) -> Iterator[CheckpointedBatchItem[PayloadT]]:
        if not isinstance(checkpoint, dict):
            raise TypeError("parquet checkpoint must be a JSON object")
        row_group_index = checkpoint.get("row_group_index", 0)
        row_index_within_group = checkpoint.get("row_index_within_group", 0)
        if not isinstance(row_group_index, int) or row_group_index < 0:
            raise ValueError("parquet checkpoint row_group_index must be >= 0")
        if not isinstance(row_index_within_group, int) or row_index_within_group < 0:
            raise ValueError("parquet checkpoint row_index_within_group must be >= 0")

        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(self.path)
        source_ref = str(self.path.resolve())
        for current_row_group_index in range(row_group_index, parquet_file.num_row_groups):
            table = parquet_file.read_row_group(current_row_group_index, columns=self.columns)
            payload_by_column = table.to_pydict()
            row_start = row_index_within_group if current_row_group_index == row_group_index else 0
            for current_row_index in range(row_start, table.num_rows):
                row = _normalized_parquet_row(payload_by_column, current_row_index)
                metadata = (
                    self.metadata_from_row(row)
                    if self.metadata_from_row is not None
                    else {}
                )
                row_id = self.item_id_from_row(row)
                next_checkpoint = _parquet_checkpoint_payload(
                    row_group_index=current_row_group_index,
                    row_index_within_group=current_row_index + 1,
                )
                if current_row_index + 1 >= table.num_rows:
                    next_checkpoint = _parquet_checkpoint_payload(
                        row_group_index=current_row_group_index + 1,
                        row_index_within_group=0,
                    )
                yield CheckpointedBatchItem(
                    next_checkpoint=next_checkpoint,
                    item=BatchItem(
                        item_id=row_id,
                        payload=self.payload_from_row(row),
                        metadata=_lineage_metadata(
                            metadata,
                            source_ref=source_ref,
                            source_item_index=sum(
                                parquet_file.metadata.row_group(index).num_rows
                                for index in range(current_row_group_index)
                            )
                            + current_row_index,
                            source_primary_key=row_id,
                            partition_id=str(current_row_group_index),
                        ),
                    ),
                )
