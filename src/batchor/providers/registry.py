"""Provider registry for runtime dispatch and config round-tripping.

The :class:`ProviderRegistry` maps :class:`~batchor.ProviderKind` values to
factory callables that create :class:`~batchor.BatchProvider` instances, and to
loader callables that deserialise persisted :class:`~batchor.ProviderConfig`
objects from JSON.  This allows the storage layer to reconstruct the correct
provider config when resuming a run in a fresh process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, cast

from batchor.core.enums import ProviderKind
from batchor.core.types import JSONObject
from batchor.providers.base import BatchProvider, ProviderConfig

if TYPE_CHECKING:
    from batchor.core.models import OpenAIProviderConfig

type ProviderFactory = Callable[[ProviderConfig], BatchProvider]
type ProviderConfigLoader = Callable[[JSONObject], ProviderConfig]


class ProviderRegistry:
    """Dispatch table mapping provider kinds to factory and loader callables.

    Use :func:`build_default_provider_registry` to obtain a registry pre-loaded
    with the built-in OpenAI provider, or construct one manually to register
    custom providers.
    """

    def __init__(self) -> None:
        self._factories: dict[ProviderKind, ProviderFactory] = {}
        self._loaders: dict[ProviderKind, ProviderConfigLoader] = {}

    def register(
        self,
        *,
        kind: ProviderKind,
        factory: ProviderFactory,
        loader: ProviderConfigLoader,
    ) -> None:
        """Register a provider factory and config loader for *kind*.

        Args:
            kind: The :class:`~batchor.ProviderKind` this registration covers.
            factory: Callable ``(config) -> BatchProvider`` that constructs a
                live provider instance from its config.
            loader: Callable ``(payload) -> ProviderConfig`` that deserialises
                a persisted config payload produced by
                :meth:`~batchor.ProviderConfig.to_payload`.
        """
        self._factories[kind] = factory
        self._loaders[kind] = loader

    def create(self, config: ProviderConfig) -> BatchProvider:
        """Create a live provider instance from *config*.

        Args:
            config: A :class:`~batchor.ProviderConfig` instance whose
                ``provider_kind`` has been registered.

        Returns:
            A ready-to-use :class:`~batchor.BatchProvider`.

        Raises:
            ValueError: If ``config.provider_kind`` is not registered.
        """
        try:
            factory = self._factories[config.provider_kind]
        except KeyError as exc:
            raise ValueError(f"unsupported provider kind: {config.provider_kind}") from exc
        return factory(config)

    def dump_config(
        self,
        config: ProviderConfig,
        *,
        include_secrets: bool = True,
    ) -> JSONObject:
        """Serialise *config* to a JSON object with a ``provider_kind`` envelope.

        Args:
            config: Provider config to serialise.
            include_secrets: When ``True`` (default), uses
                :meth:`~batchor.ProviderConfig.to_payload`.  When ``False``,
                uses :meth:`~batchor.ProviderConfig.to_public_payload`.

        Returns:
            A ``JSONObject`` with ``"provider_kind"`` and ``"config"`` keys.
        """
        config_payload = (
            config.to_payload() if include_secrets else config.to_public_payload()
        )
        return {
            "provider_kind": config.provider_kind.value,
            "config": config_payload,
        }

    def load_config(self, payload: JSONObject) -> ProviderConfig:
        """Deserialise a persisted config payload produced by :meth:`dump_config`.

        Args:
            payload: A ``JSONObject`` with ``"provider_kind"`` and ``"config"``
                keys as produced by :meth:`dump_config`.

        Returns:
            A reconstructed :class:`~batchor.ProviderConfig` instance.

        Raises:
            TypeError: If ``provider_kind`` or ``config`` have incorrect types.
            ValueError: If the ``provider_kind`` is not registered.
        """
        raw_kind = payload.get("provider_kind")
        if not isinstance(raw_kind, str):
            raise TypeError("provider_kind must be a string")
        try:
            kind = ProviderKind(raw_kind)
        except ValueError as exc:
            raise ValueError(f"unsupported provider kind: {raw_kind}") from exc
        raw_config = payload.get("config")
        if not isinstance(raw_config, dict):
            raise TypeError("config must be a JSON object")
        try:
            loader = self._loaders[kind]
        except KeyError as exc:
            raise ValueError(f"unsupported provider kind: {kind}") from exc
        return loader(cast(JSONObject, raw_config))


def build_default_provider_registry() -> ProviderRegistry:
    """Create a :class:`ProviderRegistry` pre-loaded with the OpenAI provider.

    Returns:
        A :class:`ProviderRegistry` with :attr:`~batchor.ProviderKind.OPENAI`
        registered.
    """
    from batchor.core.models import OpenAIProviderConfig
    from batchor.providers.openai import OpenAIBatchProvider

    registry = ProviderRegistry()
    registry.register(
        kind=ProviderKind.OPENAI,
        factory=lambda config: OpenAIBatchProvider(_require_openai_config(config)),
        loader=OpenAIProviderConfig.from_payload,
    )
    return registry


def _require_openai_config(config: ProviderConfig) -> OpenAIProviderConfig:
    from batchor.core.models import OpenAIProviderConfig

    if not isinstance(config, OpenAIProviderConfig):
        raise TypeError(
            f"expected {ProviderKind.OPENAI.value} config, got {type(config).__name__}"
        )
    return config
