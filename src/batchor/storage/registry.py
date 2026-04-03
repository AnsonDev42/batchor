from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from batchor.core.enums import StorageKind
from batchor.storage.state import MemoryStateStore, StateStore

if TYPE_CHECKING:
    from batchor.providers.registry import ProviderRegistry


type StorageFactory = Callable[[], StateStore]


class StorageRegistry:
    def __init__(self) -> None:
        self._factories: dict[StorageKind, StorageFactory] = {}

    def register(self, *, kind: StorageKind, factory: StorageFactory) -> None:
        self._factories[kind] = factory

    def create(self, kind: StorageKind | str) -> StateStore:
        resolved_kind = StorageKind(kind)
        try:
            factory = self._factories[resolved_kind]
        except KeyError as exc:
            raise ValueError(f"unsupported storage backend: {resolved_kind}") from exc
        return factory()


def build_default_storage_registry(
    *,
    provider_registry: ProviderRegistry | None = None,
) -> StorageRegistry:
    from batchor.storage.postgres import PostgresStorage
    from batchor.storage.sqlite import SQLiteStorage

    registry = StorageRegistry()
    registry.register(
        kind=StorageKind.SQLITE,
        factory=lambda: SQLiteStorage(name="default", provider_registry=provider_registry),
    )
    registry.register(
        kind=StorageKind.POSTGRES,
        factory=lambda: PostgresStorage.from_env(provider_registry=provider_registry),
    )
    registry.register(kind=StorageKind.MEMORY, factory=MemoryStateStore)
    return registry
