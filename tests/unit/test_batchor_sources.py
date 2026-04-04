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


# ---------------------------------------------------------------------------
# CSV: negative item_index and skip-ahead resumption
# ---------------------------------------------------------------------------


def test_csv_item_source_rejects_negative_item_index(tmp_path: Path) -> None:
    path = tmp_path / "items.csv"
    path.write_text("id,text\nr1,alpha\n", encoding="utf-8")
    source = CsvItemSource(path, item_id_from_row=lambda row: row["id"], payload_from_row=lambda row: row)
    import pytest

    with pytest.raises(ValueError, match="item_index must be >= 0"):
        list(source.iter_from(-1))


def test_csv_item_source_can_resume_from_item_index(tmp_path: Path) -> None:
    path = tmp_path / "items.csv"
    path.write_text("id,text\nr1,alpha\nr2,beta\nr3,gamma\n", encoding="utf-8")
    source = CsvItemSource(
        path, item_id_from_row=lambda row: row["id"], payload_from_row=lambda row: {"text": row["text"]}
    )
    resumed = list(source.iter_from(1))
    assert [item.item_index for item in resumed] == [1, 2]
    assert [item.item.item_id for item in resumed] == ["r2", "r3"]


# ---------------------------------------------------------------------------
# JSONL: negative item_index, blank lines, invalid JSON
# ---------------------------------------------------------------------------


def test_jsonl_item_source_rejects_negative_item_index(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    path.write_text('{"id":"r1"}\n', encoding="utf-8")
    source = JsonlItemSource(
        path, item_id_from_row=lambda r: str(r["id"]) if isinstance(r, dict) else "", payload_from_row=lambda r: r
    )
    import pytest

    with pytest.raises(ValueError, match="item_index must be >= 0"):
        list(source.iter_from(-1))


def test_jsonl_item_source_skips_blank_lines_on_resume(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    path.write_text('{"id":"r1"}\n\n{"id":"r2"}\n{"id":"r3"}\n', encoding="utf-8")
    source = JsonlItemSource(
        path,
        item_id_from_row=lambda r: str(r["id"]) if isinstance(r, dict) else "",
        payload_from_row=lambda r: r,
    )
    # Full iteration: blank line should not be counted
    all_items = list(source.iter_from(0))
    assert [i.item.item_id for i in all_items] == ["r1", "r2", "r3"]
    assert [i.item_index for i in all_items] == [0, 1, 2]

    # Resume from index 1: blank line before r2 must not shift index
    resumed = list(source.iter_from(1))
    assert [i.item.item_id for i in resumed] == ["r2", "r3"]
    assert [i.item_index for i in resumed] == [1, 2]


def test_jsonl_item_source_raises_on_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text('{"id":"r1"}\n{not valid json}\n', encoding="utf-8")
    source = JsonlItemSource(
        path,
        item_id_from_row=lambda r: str(r["id"]) if isinstance(r, dict) else "",
        payload_from_row=lambda r: r,
    )
    import pytest

    with pytest.raises(ValueError, match="invalid JSONL record"):
        list(source)


# ---------------------------------------------------------------------------
# _lineage_metadata edge cases
# ---------------------------------------------------------------------------


def test_lineage_metadata_handles_explicit_none_batchor_lineage(tmp_path: Path) -> None:
    """metadata={"batchor_lineage": None} should be treated as empty lineage."""
    path = tmp_path / "items.csv"
    path.write_text("id,text\nr1,alpha\n", encoding="utf-8")

    from batchor.sources.files import _lineage_metadata

    result = _lineage_metadata(
        {"batchor_lineage": None},
        source_ref="test",
        source_item_index=0,
    )
    assert result["batchor_lineage"]["source_ref"] == "test"


def test_lineage_metadata_rejects_non_dict_batchor_lineage(tmp_path: Path) -> None:
    import pytest

    from batchor.sources.files import _lineage_metadata

    with pytest.raises(TypeError, match="batchor_lineage"):
        _lineage_metadata(
            {"batchor_lineage": "not-a-dict"},
            source_ref="test",
            source_item_index=0,
        )


# ---------------------------------------------------------------------------
# Parquet: invalid checkpoint
# ---------------------------------------------------------------------------


def test_parquet_item_source_rejects_non_dict_checkpoint(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pytest

    path = tmp_path / "items.parquet"
    table = pa.table({"id": ["r1"], "text": ["alpha"]})
    pq.write_table(table, path)

    source = ParquetItemSource(
        path,
        item_id_from_row=lambda row: str(row["id"]),
        payload_from_row=lambda row: {"text": row["text"]},
    )
    with pytest.raises(TypeError, match="parquet checkpoint must be a JSON object"):
        list(source.iter_from_checkpoint("not-a-dict"))


# ---------------------------------------------------------------------------
# Parquet: invalid checkpoint values (row_group_index / row_index_within_group)
# ---------------------------------------------------------------------------


def test_parquet_item_source_rejects_negative_row_group_index(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pytest

    path = tmp_path / "items.parquet"
    pq.write_table(pa.table({"id": ["r1"], "text": ["alpha"]}), path)
    source = ParquetItemSource(path, item_id_from_row=lambda r: str(r["id"]), payload_from_row=lambda r: r)
    with pytest.raises(ValueError, match="row_group_index must be >= 0"):
        list(source.iter_from_checkpoint({"row_group_index": -1, "row_index_within_group": 0}))


def test_parquet_item_source_rejects_negative_row_index_within_group(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pytest

    path = tmp_path / "items.parquet"
    pq.write_table(pa.table({"id": ["r1"], "text": ["alpha"]}), path)
    source = ParquetItemSource(path, item_id_from_row=lambda r: str(r["id"]), payload_from_row=lambda r: r)
    with pytest.raises(ValueError, match="row_index_within_group must be >= 0"):
        list(source.iter_from_checkpoint({"row_group_index": 0, "row_index_within_group": -1}))


# ---------------------------------------------------------------------------
# CompositeItemSource: checkpoint validation errors
# ---------------------------------------------------------------------------


def test_composite_item_source_rejects_non_dict_checkpoint() -> None:
    import pytest

    source_a = _StaticCheckpointedSource(name="a", items=[BatchItem(item_id="r1", payload={})])
    composite = CompositeItemSource([source_a])
    with pytest.raises(TypeError, match="composite checkpoint must be a JSON object"):
        list(composite.iter_from_checkpoint("invalid"))


def test_composite_item_source_rejects_out_of_range_source_index() -> None:
    import pytest

    source_a = _StaticCheckpointedSource(name="a", items=[BatchItem(item_id="r1", payload={})])
    composite = CompositeItemSource([source_a])
    with pytest.raises(ValueError, match="source_index is out of range"):
        list(composite.iter_from_checkpoint({"source_index": 99, "child_checkpoint": None}))


def test_composite_item_source_rejects_non_int_source_index() -> None:
    import pytest

    source_a = _StaticCheckpointedSource(name="a", items=[BatchItem(item_id="r1", payload={})])
    composite = CompositeItemSource([source_a])
    with pytest.raises(TypeError, match="source_index must be an int"):
        list(composite.iter_from_checkpoint({"source_index": "bad", "child_checkpoint": None}))


def test_composite_item_source_rejects_non_null_child_checkpoint_at_end() -> None:
    import pytest

    source_a = _StaticCheckpointedSource(name="a", items=[BatchItem(item_id="r1", payload={})])
    composite = CompositeItemSource([source_a])
    # source_index == len(sources) == 1, but child_checkpoint is not None
    with pytest.raises(ValueError, match="child_checkpoint must be null at end"):
        list(composite.iter_from_checkpoint({"source_index": 1, "child_checkpoint": 42}))


def test_composite_metadata_rejects_non_dict_batchor_lineage() -> None:
    """An item with batchor_lineage set to a non-dict value should raise TypeError."""
    import pytest

    source_a = _StaticCheckpointedSource(
        name="a",
        items=[
            BatchItem(
                item_id="r1",
                payload={},
                metadata={"batchor_lineage": "not-a-dict"},
            )
        ],
    )
    composite = CompositeItemSource([source_a])
    with pytest.raises(TypeError, match="batchor_lineage"):
        list(composite)


def test_composite_metadata_handles_none_batchor_lineage() -> None:
    """An item with batchor_lineage=None in metadata should be treated as empty lineage."""
    source_a = _StaticCheckpointedSource(
        name="a",
        items=[
            BatchItem(
                item_id="r1",
                payload={},
                metadata={"batchor_lineage": None},
            )
        ],
    )
    composite = CompositeItemSource([source_a])
    items = list(composite)
    assert items[0].metadata["batchor_lineage"]["source_primary_key"] == "r1"


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
