from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Sequence
from typing import Generic, TypeVar, cast

from batchor.core.models import BatchItem
from batchor.core.types import JSONObject, JSONValue
from batchor.sources.base import (
    CheckpointedBatchItem,
    CheckpointedItemSource,
    SourceIdentity,
)

PayloadT = TypeVar("PayloadT")


def _identity_payload(identity: SourceIdentity) -> JSONObject:
    return {
        "source_kind": identity.source_kind,
        "source_ref": identity.source_ref,
        "source_fingerprint": identity.source_fingerprint,
    }


def _canonical_json(payload: JSONValue) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _composite_checkpoint_payload(
    *,
    source_index: int,
    child_checkpoint: JSONValue | None,
) -> JSONObject:
    return {
        "source_index": source_index,
        "child_checkpoint": child_checkpoint,
    }


def _source_namespace(source_index: int, identity: SourceIdentity) -> str:
    payload = f"{source_index}\0{identity.source_kind}\0{identity.source_ref}".encode()
    return f"src_{hashlib.sha256(payload).hexdigest()[:12]}"


def _lineage_metadata(
    metadata: JSONObject,
    *,
    original_item_id: str,
    source_namespace: str,
) -> JSONObject:
    normalized = dict(metadata)
    lineage = normalized.get("batchor_lineage", {})
    if lineage is None:
        lineage = {}
    if not isinstance(lineage, dict):
        raise TypeError("batchor_lineage metadata must be a JSON object")
    lineage_payload: dict[str, JSONValue] = {str(key): cast(JSONValue, value) for key, value in lineage.items()}
    lineage_payload["source_namespace"] = source_namespace
    lineage_payload["source_primary_key"] = original_item_id
    normalized["batchor_lineage"] = cast(JSONObject, lineage_payload)
    return normalized


class CompositeItemSource(CheckpointedItemSource[PayloadT], Generic[PayloadT]):
    """Compose ordered checkpointed sources into one logical deterministic source."""

    def __init__(self, sources: Sequence[CheckpointedItemSource[PayloadT]]) -> None:
        self.sources = tuple(sources)
        self._identities = tuple(source.source_identity() for source in self.sources)
        identity_payload: JSONValue = [_identity_payload(identity) for identity in self._identities]
        self._source_ref = _canonical_json(identity_payload)
        self._source_fingerprint = hashlib.sha256(self._source_ref.encode("utf-8")).hexdigest()
        self._source_namespaces = tuple(
            _source_namespace(index, identity) for index, identity in enumerate(self._identities)
        )

    def source_identity(self) -> SourceIdentity:
        return SourceIdentity(
            source_kind="composite",
            source_ref=self._source_ref,
            source_fingerprint=self._source_fingerprint,
        )

    def initial_checkpoint(self) -> JSONValue:
        return _composite_checkpoint_payload(
            source_index=0,
            child_checkpoint=None,
        )

    def iter_from_checkpoint(
        self,
        checkpoint: JSONValue,
    ) -> Iterator[CheckpointedBatchItem[PayloadT]]:
        source_index, child_checkpoint = self._parse_checkpoint(checkpoint)
        for current_source_index in range(source_index, len(self.sources)):
            source = self.sources[current_source_index]
            source_iterator = iter(
                source.iter_from_checkpoint(
                    source.initial_checkpoint()
                    if current_source_index != source_index or child_checkpoint is None
                    else child_checkpoint
                )
            )
            current = next(source_iterator, None)
            while current is not None:
                next_item = next(source_iterator, None)
                next_checkpoint = _composite_checkpoint_payload(
                    source_index=current_source_index,
                    child_checkpoint=current.next_checkpoint,
                )
                if next_item is None:
                    next_checkpoint = _composite_checkpoint_payload(
                        source_index=current_source_index + 1,
                        child_checkpoint=None,
                    )
                yield CheckpointedBatchItem(
                    next_checkpoint=next_checkpoint,
                    item=self._namespaced_item(
                        current.item,
                        source_index=current_source_index,
                    ),
                )
                current = next_item

    def _parse_checkpoint(self, checkpoint: JSONValue) -> tuple[int, JSONValue | None]:
        if not isinstance(checkpoint, dict):
            raise TypeError("composite checkpoint must be a JSON object")
        source_index = checkpoint.get("source_index", 0)
        child_checkpoint = checkpoint.get("child_checkpoint")
        if not isinstance(source_index, int):
            raise TypeError("composite checkpoint source_index must be an int")
        if source_index < 0 or source_index > len(self.sources):
            raise ValueError("composite checkpoint source_index is out of range")
        if source_index == len(self.sources) and child_checkpoint is not None:
            raise ValueError("composite checkpoint child_checkpoint must be null at end of sources")
        return source_index, cast(JSONValue | None, child_checkpoint)

    def _namespaced_item(
        self,
        item: BatchItem[PayloadT],
        *,
        source_index: int,
    ) -> BatchItem[PayloadT]:
        source_namespace = self._source_namespaces[source_index]
        return BatchItem(
            item_id=f"{source_namespace}__{item.item_id}",
            payload=item.payload,
            metadata=_lineage_metadata(
                item.metadata,
                original_item_id=item.item_id,
                source_namespace=source_namespace,
            ),
        )
