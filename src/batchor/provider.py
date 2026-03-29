from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable, cast

from batchor.enums import ProviderKind
from batchor.types import BatchRemoteRecord, BatchRequestLine, JSONObject

if TYPE_CHECKING:
    from batchor.models import OpenAIProviderConfig, PromptParts


@dataclass(frozen=True)
class StructuredOutputSchema:
    name: str
    schema: JSONObject


class ProviderConfig(ABC):
    poll_interval_sec: float

    @property
    @abstractmethod
    def provider_kind(self) -> ProviderKind:
        """Stable provider identifier used for runtime dispatch and persistence."""

    @abstractmethod
    def to_payload(self) -> JSONObject:
        """Serialize provider-specific config to JSON for durable storage."""


class BatchProvider(ABC):
    @abstractmethod
    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output: StructuredOutputSchema | None = None,
    ) -> BatchRequestLine: ...

    @abstractmethod
    def write_requests_jsonl(
        self,
        request_lines: list[BatchRequestLine],
        output_path: str | Path,
    ) -> Path: ...

    @abstractmethod
    def upload_input_file(self, input_path: str | Path) -> str: ...

    @abstractmethod
    def create_batch(
        self,
        *,
        input_file_id: str,
        metadata: dict[str, str] | None = None,
    ) -> BatchRemoteRecord: ...

    @abstractmethod
    def get_batch(self, batch_id: str) -> BatchRemoteRecord: ...

    @abstractmethod
    def download_file_content(self, file_id: str) -> str: ...

    @abstractmethod
    def parse_batch_output(
        self,
        *,
        output_content: str | None,
        error_content: str | None,
    ) -> tuple[dict[str, JSONObject], dict[str, JSONObject], list[JSONObject]]: ...

    @abstractmethod
    def estimate_request_tokens(
        self,
        request_line: BatchRequestLine,
        *,
        chars_per_token: int,
    ) -> int: ...


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

    def dump_config(self, config: ProviderConfig) -> JSONObject:
        return {
            "provider_kind": config.provider_kind.value,
            "config": config.to_payload(),
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
    from batchor.models import OpenAIProviderConfig
    from batchor.openai_provider import OpenAIBatchProvider

    registry = ProviderRegistry()
    registry.register(
        kind=ProviderKind.OPENAI,
        factory=lambda config: OpenAIBatchProvider(_require_openai_config(config)),
        loader=OpenAIProviderConfig.from_payload,
    )
    return registry


def _require_openai_config(config: ProviderConfig) -> OpenAIProviderConfig:
    from batchor.models import OpenAIProviderConfig

    if not isinstance(config, OpenAIProviderConfig):
        raise TypeError(
            f"expected {ProviderKind.OPENAI.value} config, got {type(config).__name__}"
        )
    return config
