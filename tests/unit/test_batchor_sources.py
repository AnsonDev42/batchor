from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from batchor.core.models import BatchItem
from batchor.core.types import JSONValue
from batchor.sources.base import (
    CheckpointedBatchItem,
    CheckpointedItemSource,
    SourceIdentity,
)
from batchor.sources.composite import CompositeItemSource
from batchor.sources.files import CsvItemSource, JsonlItemSource, ParquetItemSource


class _StaticCheckpointedSource(CheckpointedItemSource[dict[str, str]]):
    def __init__(self, *, name: str, items: list[BatchItem[dict[str, str]]]) -> None:
        self.name = name
        self.items = list(items)

    def source_identity(self) -> SourceIdentity:
        return SourceIdentity(
            source_kind="static",
            source_ref=self.name,
            source_fingerprint=f"fingerprint-{self.name}",
        )

    def initial_checkpoint(self) -> JSONValue:
        return 0

    def iter_from_checkpoint(
        self,
        checkpoint: JSONValue,
    ) -> Iterator[CheckpointedBatchItem[dict[str, str]]]:
        if not isinstance(checkpoint, int):
            raise TypeError("static checkpoint must be an int")
        for index, item in enumerate(self.items[checkpoint:], start=checkpoint):
            yield CheckpointedBatchItem(
                next_checkpoint=index + 1,
                item=item,
            )


def test_csv_item_source_maps_rows_to_batch_items(tmp_path: Path) -> None:
    path = tmp_path / "items.csv"
    path.write_text("id,text,source\nr1,alpha,a\nr2,beta,b\n", encoding="utf-8")

    source = CsvItemSource(
        path,
        item_id_from_row=lambda row: row["id"],
        payload_from_row=lambda row: {"text": row["text"]},
        metadata_from_row=lambda row: {"source": row["source"]},
    )

    items = list(source)
    assert [item.item_id for item in items] == ["r1", "r2"]
    assert items[0].payload == {"text": "alpha"}
    assert items[1].metadata["source"] == "b"
    assert items[1].metadata["batchor_lineage"]["source_item_index"] == 1


def test_jsonl_item_source_maps_rows_to_batch_items(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    records = [
        {"id": "r1", "text": "alpha", "source": "a"},
        {"id": "r2", "text": "beta", "source": "b"},
    ]
    path.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )

    source = JsonlItemSource(
        path,
        item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
        payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
        metadata_from_row=lambda row: {"source": row["source"]} if isinstance(row, dict) else {},
    )

    items = list(source)
    assert [item.item_id for item in items] == ["r1", "r2"]
    assert items[0].payload == {"text": "alpha"}
    assert items[1].metadata["source"] == "b"
    assert items[1].metadata["batchor_lineage"]["source_item_index"] == 1


def test_jsonl_item_source_can_resume_from_item_index(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    records = [
        {"id": "r1", "text": "alpha"},
        {"id": "r2", "text": "beta"},
        {"id": "r3", "text": "gamma"},
    ]
    path.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )

    source = JsonlItemSource(
        path,
        item_id_from_row=lambda row: str(row["id"]) if isinstance(row, dict) else "",
        payload_from_row=lambda row: {"text": row["text"]} if isinstance(row, dict) else {},
    )

    resumed = list(source.iter_from(1))
    assert [item.item_index for item in resumed] == [1, 2]
    assert [item.item.item_id for item in resumed] == ["r2", "r3"]


def test_csv_item_source_exposes_stable_source_identity(tmp_path: Path) -> None:
    path = tmp_path / "items.csv"
    path.write_text("id,text\nr1,alpha\n", encoding="utf-8")

    source = CsvItemSource(
        path,
        item_id_from_row=lambda row: row["id"],
        payload_from_row=lambda row: {"text": row["text"]},
    )

    identity = source.source_identity()
    assert identity.source_kind == "csv"
    assert identity.source_ref == str(path.resolve())
    assert identity.source_fingerprint


def test_parquet_item_source_projects_columns_and_emits_lineage(tmp_path: Path) -> None:
    path = tmp_path / "items.parquet"
    table = pa.table(
        {
            "id": ["r1", "r2"],
            "text": ["alpha", "beta"],
            "source": ["a", "b"],
            "unused": [1, 2],
        }
    )
    pq.write_table(table, path)

    source = ParquetItemSource(
        path,
        item_id_from_row=lambda row: str(row["id"]),
        payload_from_row=lambda row: {"text": row["text"]},
        metadata_from_row=lambda row: {"source": str(row["source"])},
        columns=["id", "text", "source"],
    )

    items = list(source)
    assert [item.item_id for item in items] == ["r1", "r2"]
    assert items[0].payload == {"text": "alpha"}
    assert items[1].metadata["source"] == "b"
    assert items[1].metadata["batchor_lineage"]["partition_id"] == "0"


def test_parquet_item_source_resumes_from_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "items.parquet"
    table = pa.table(
        {
            "id": ["r1", "r2", "r3"],
            "text": ["alpha", "beta", "gamma"],
        }
    )
    pq.write_table(table, path, row_group_size=1)

    source = ParquetItemSource(
        path,
        item_id_from_row=lambda row: str(row["id"]),
        payload_from_row=lambda row: {"text": row["text"]},
        columns=["id", "text"],
    )

    initial = list(source.iter_from_checkpoint(source.initial_checkpoint()))
    assert [item.item.item_id for item in initial] == ["r1", "r2", "r3"]

    resumed = list(
        source.iter_from_checkpoint(
            {
                "row_group_index": 1,
                "row_index_within_group": 0,
            }
        )
    )
    assert [item.item.item_id for item in resumed] == ["r2", "r3"]


def test_composite_item_source_namespaces_item_ids_and_lineage() -> None:
    source_a = _StaticCheckpointedSource(
        name="a",
        items=[
            BatchItem(
                item_id="row1",
                payload={"text": "alpha"},
                metadata={
                    "source": "a",
                    "batchor_lineage": {
                        "source_ref": "input-a",
                        "source_item_index": 0,
                    },
                },
            ),
        ],
    )
    source_b = _StaticCheckpointedSource(
        name="b",
        items=[
            BatchItem(
                item_id="row1",
                payload={"text": "beta"},
                metadata={
                    "source": "b",
                    "batchor_lineage": {
                        "source_ref": "input-b",
                        "source_item_index": 0,
                    },
                },
            ),
        ],
    )

    composite = CompositeItemSource([source_a, source_b])

    items = list(composite)
    assert len(items) == 2
    assert items[0].item_id.endswith("__row1")
    assert items[1].item_id.endswith("__row1")
    assert items[0].item_id != items[1].item_id
    assert items[0].metadata["source"] == "a"
    assert items[1].metadata["source"] == "b"
    assert items[0].metadata["batchor_lineage"]["source_primary_key"] == "row1"
    assert items[1].metadata["batchor_lineage"]["source_primary_key"] == "row1"
    assert items[0].metadata["batchor_lineage"]["source_namespace"].startswith("src_")
    assert items[1].metadata["batchor_lineage"]["source_namespace"].startswith("src_")
    assert items[0].metadata["batchor_lineage"]["source_ref"] == "input-a"
    assert items[1].metadata["batchor_lineage"]["source_ref"] == "input-b"


def test_composite_item_source_skips_empty_children_and_resumes_across_boundaries() -> None:
    source_a = _StaticCheckpointedSource(
        name="a",
        items=[
            BatchItem(item_id="a1", payload={"text": "alpha"}),
            BatchItem(item_id="a2", payload={"text": "beta"}),
        ],
    )
    source_b = _StaticCheckpointedSource(name="b", items=[])
    source_c = _StaticCheckpointedSource(
        name="c",
        items=[BatchItem(item_id="c1", payload={"text": "gamma"})],
    )

    composite = CompositeItemSource([source_a, source_b, source_c])
    checkpointed_items = list(composite.iter_from_checkpoint(composite.initial_checkpoint()))
    first_namespace = checkpointed_items[0].item.item_id.split("__", maxsplit=1)[0]
    third_namespace = checkpointed_items[2].item.item_id.split("__", maxsplit=1)[0]

    assert [item.item.item_id for item in checkpointed_items] == [
        f"{first_namespace}__a1",
        f"{first_namespace}__a2",
        f"{third_namespace}__c1",
    ]
    assert first_namespace.startswith("src_")
    assert third_namespace.startswith("src_")
    assert first_namespace != third_namespace
    assert checkpointed_items[0].next_checkpoint == {
        "source_index": 0,
        "child_checkpoint": 1,
    }
    assert checkpointed_items[1].next_checkpoint == {
        "source_index": 1,
        "child_checkpoint": None,
    }
    resumed = list(composite.iter_from_checkpoint(checkpointed_items[1].next_checkpoint))
    assert [item.item.item_id for item in resumed] == [f"{third_namespace}__c1"]


def test_composite_item_source_exposes_stable_identity() -> None:
    source_a = _StaticCheckpointedSource(
        name="a",
        items=[BatchItem(item_id="row1", payload={"text": "alpha"})],
    )
    source_b = _StaticCheckpointedSource(
        name="b",
        items=[BatchItem(item_id="row2", payload={"text": "beta"})],
    )

    left = CompositeItemSource([source_a, source_b]).source_identity()
    right = CompositeItemSource([source_a, source_b]).source_identity()
    reversed_identity = CompositeItemSource([source_b, source_a]).source_identity()

    assert left.source_kind == "composite"
    assert left.source_ref == right.source_ref
    assert left.source_fingerprint == right.source_fingerprint
    assert left.source_ref != reversed_identity.source_ref
    assert left.source_fingerprint != reversed_identity.source_fingerprint
