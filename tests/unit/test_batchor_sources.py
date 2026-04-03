from __future__ import annotations

import json
from pathlib import Path

from batchor.sources.files import CsvItemSource, JsonlItemSource


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
    assert items[1].metadata == {"source": "b"}


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
    assert items[1].metadata == {"source": "b"}


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
