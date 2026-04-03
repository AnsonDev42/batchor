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
        self._factories[kind] = factory
        self._loaders[kind] = loader

    def create(self, config: ProviderConfig) -> BatchProvider:
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
        config_payload = (
            config.to_payload() if include_secrets else config.to_public_payload()
        )
        return {
            "provider_kind": config.provider_kind.value,
            "config": config_payload,
        }

    def load_config(self, payload: JSONObject) -> ProviderConfig:
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
