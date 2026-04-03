"""Public re-exports for the state-store layer.

Consumers should import from this module rather than from the internal
sub-modules (:mod:`batchor.storage.state_models`,
:mod:`batchor.storage.memory`) so that the internal structure can change
without breaking callers.
"""

from batchor.storage.memory import MemoryStateStore, serialize_item_failure
from batchor.storage.state_models import (
    ActiveBatchRecord,
    BatchArtifactPointer,
    ClaimedItem,
    CompletedItemRecord,
    IngestCheckpoint,
    ItemFailureRecord,
    MaterializedItem,
    PersistedItemRecord,
    PersistedRunConfig,
    PreparedSubmission,
    QueuedItemFailureRecord,
    RequestArtifactPointer,
    RetryBackoffState,
    RunArtifactInventory,
    StateStore,
)

__all__ = [
    "ActiveBatchRecord",
    "BatchArtifactPointer",
    "ClaimedItem",
    "CompletedItemRecord",
    "IngestCheckpoint",
    "ItemFailureRecord",
    "MaterializedItem",
    "MemoryStateStore",
    "PersistedItemRecord",
    "PersistedRunConfig",
    "PreparedSubmission",
    "QueuedItemFailureRecord",
    "RequestArtifactPointer",
    "RetryBackoffState",
    "RunArtifactInventory",
    "StateStore",
    "serialize_item_failure",
]
