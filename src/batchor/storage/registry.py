"""Storage registry for runtime dispatch across backend implementations.

The :class:`StorageRegistry` maps :class:`~batchor.StorageKind` values to
factory callables that create :class:`~batchor.StateStore` instances.  This
allows :class:`~batchor.BatchRunner` to accept a backend name string (e.g.
``"sqlite"`` or ``"postgres"``) without hard-coding the concrete classes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from batchor.core.enums import StorageKind
from batchor.storage.state import MemoryStateStore, StateStore

if TYPE_CHECKING:
    from batchor.providers.registry import ProviderRegistry


type StorageFactory = Callable[[], StateStore]


class StorageRegistry:
    """Dispatch table mapping storage kinds to factory callables.

    Use :func:`build_default_storage_registry` to obtain a registry
    pre-loaded with SQLite, Postgres, and in-memory backends, or construct
    one manually to register custom backends.
    """

    def __init__(self) -> None:
        self._factories: dict[StorageKind, StorageFactory] = {}

    def register(self, *, kind: StorageKind, factory: StorageFactory) -> None:
        """Register a storage factory for *kind*.

        Args:
            kind: The :class:`~batchor.StorageKind` this factory handles.
            factory: Zero-argument callable that returns a
                :class:`~batchor.StateStore` instance.
        """
        self._factories[kind] = factory

    def create(self, kind: StorageKind | str) -> StateStore:
        """Create a storage backend of the given kind.

        Args:
            kind: A :class:`~batchor.StorageKind` value or equivalent string.

        Returns:
            A ready-to-use :class:`~batchor.StateStore`.

        Raises:
            ValueError: If *kind* is not registered or is not a valid
                :class:`~batchor.StorageKind`.
        """
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
    """Create a :class:`StorageRegistry` pre-loaded with all built-in backends.

    Registers:

    * ``StorageKind.SQLITE`` → :class:`~batchor.SQLiteStorage` (default store)
    * ``StorageKind.POSTGRES`` → :class:`~batchor.PostgresStorage`
    * ``StorageKind.MEMORY`` → :class:`~batchor.MemoryStateStore`

    Args:
        provider_registry: Optional provider registry forwarded to backends
            that need it for config round-tripping on resume.

    Returns:
        A :class:`StorageRegistry` with all built-in backends registered.
    """
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
