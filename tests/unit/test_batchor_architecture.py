from __future__ import annotations

from pathlib import Path

from batchor import (
    MemoryStateStore,
    OpenAIEnqueueLimitConfig,
    OpenAIProviderConfig,
    OpenAIBatchProvider,
    ProviderKind,
    StorageKind,
    build_default_provider_registry,
)
from batchor.storage.registry import StorageRegistry
from batchor.storage.sqlite import SQLiteStorage


def test_default_provider_registry_round_trips_openai_config() -> None:
    registry = build_default_provider_registry()
    config = OpenAIProviderConfig(
        api_key="k",
        model="gpt-4.1",
        enqueue_limits=OpenAIEnqueueLimitConfig(
            enqueued_token_limit=1000,
            target_ratio=0.7,
            headroom=10,
            max_batch_enqueued_tokens=500,
        ),
    )

    payload = registry.dump_config(config)
    loaded = registry.load_config(payload)
    provider = registry.create(loaded)

    assert loaded.provider_kind is ProviderKind.OPENAI
    assert isinstance(loaded, OpenAIProviderConfig)
    assert loaded == config
    assert loaded.enqueue_limits.max_batch_enqueued_tokens == 500
    assert isinstance(provider, OpenAIBatchProvider)


def test_storage_registry_supports_explicit_backend_factories(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "registry.sqlite3"
    provider_registry = build_default_provider_registry()
    storage_registry = StorageRegistry()
    storage_registry.register(kind=StorageKind.MEMORY, factory=MemoryStateStore)
    storage_registry.register(
        kind=StorageKind.SQLITE,
        factory=lambda: SQLiteStorage(path=sqlite_path, provider_registry=provider_registry),
    )
    memory = storage_registry.create(StorageKind.MEMORY)
    sqlite = storage_registry.create(StorageKind.SQLITE)

    assert isinstance(memory, MemoryStateStore)
    assert isinstance(sqlite, SQLiteStorage)
    sqlite.close()
