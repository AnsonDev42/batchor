"""Internal helpers for runtime config persistence and run context creation."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Callable, cast

from pydantic import BaseModel

from batchor.core.exceptions import ModelResolutionError
from batchor.core.models import BatchJob
from batchor.providers.base import BatchProvider, StructuredOutputSchema
from batchor.providers.registry import ProviderRegistry
from batchor.runtime.validation import model_output_schema
from batchor.storage.state import PersistedRunConfig


@dataclass(frozen=True)
class RunContext:
    """Resolved execution context for one durable run.

    Attributes:
        config: Persisted run configuration for the durable run.
        provider: Provider instance used for submission and polling.
        output_model: Structured output model used for result rehydration, if
            any.
        structured_output: Provider-facing structured output schema, if any.
    """

    config: PersistedRunConfig
    provider: BatchProvider
    output_model: type[BaseModel] | None
    structured_output: StructuredOutputSchema | None


def build_persisted_config(job: BatchJob[Any, BaseModel]) -> PersistedRunConfig:
    """Build the durable run config persisted for a job.

    Args:
        job: Declarative batch job supplied by the caller.

    Returns:
        The persisted configuration stored for the durable run.
    """
    structured_output_module = None
    structured_output_qualname = None
    if job.structured_output is not None:
        structured_output_module = job.structured_output.__module__
        structured_output_qualname = job.structured_output.__qualname__
    return PersistedRunConfig(
        provider_config=job.provider_config,
        chunk_policy=job.chunk_policy,
        retry_policy=job.retry_policy,
        batch_metadata=dict(job.batch_metadata),
        artifact_policy=job.artifact_policy,
        schema_name=job.schema_name,
        structured_output_module=structured_output_module,
        structured_output_qualname=structured_output_qualname,
    )


def build_run_context(
    *,
    config: PersistedRunConfig,
    output_model: type[BaseModel] | None,
    create_provider: Callable[[Any], BatchProvider],
) -> RunContext:
    """Create the in-memory execution context for a persisted run config.

    Args:
        config: Persisted run configuration.
        output_model: Structured output model for the run, if any.
        create_provider: Factory used to instantiate the provider from the
            persisted provider config.

    Returns:
        The resolved runtime context for the run.
    """
    structured_output = None
    if output_model is not None:
        schema_name, schema = model_output_schema(
            output_model,
            schema_name=config.schema_name,
        )
        structured_output = StructuredOutputSchema(name=schema_name, schema=schema)
    return RunContext(
        config=config,
        provider=create_provider(config.provider_config),
        output_model=output_model,
        structured_output=structured_output,
    )


def configs_match_for_resume(
    *,
    stored_config: PersistedRunConfig,
    supplied_config: PersistedRunConfig,
    provider_registry: ProviderRegistry,
) -> bool:
    """Return whether a stored durable config matches a supplied job config.

    Args:
        stored_config: Config already persisted for the durable run.
        supplied_config: Config derived from the caller's supplied job.
        provider_registry: Registry used to compare provider configs without
            secrets.

    Returns:
        True if the supplied job can safely resume the stored run.
    """
    if (
        stored_config.chunk_policy != supplied_config.chunk_policy
        or stored_config.retry_policy != supplied_config.retry_policy
        or stored_config.batch_metadata != supplied_config.batch_metadata
        or stored_config.artifact_policy != supplied_config.artifact_policy
        or stored_config.schema_name != supplied_config.schema_name
        or stored_config.structured_output_module != supplied_config.structured_output_module
        or stored_config.structured_output_qualname != supplied_config.structured_output_qualname
    ):
        return False
    return provider_registry.dump_config(
        stored_config.provider_config,
        include_secrets=False,
    ) == provider_registry.dump_config(
        supplied_config.provider_config,
        include_secrets=False,
    )


def resolve_output_model(config: PersistedRunConfig) -> type[BaseModel] | None:
    """Resolve a persisted structured-output model back into a class.

    Args:
        config: Persisted run configuration.

    Returns:
        The structured output model class, or ``None`` when the run is not
        structured.

    Raises:
        ModelResolutionError: If the persisted model cannot be imported or is
            not a module-level Pydantic model.
    """
    if not config.is_structured:
        return None
    module_name = config.structured_output_module
    qualname = config.structured_output_qualname
    if module_name is None or qualname is None:
        return None
    if "<locals>" in qualname:
        raise ModelResolutionError(module_name, qualname)
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # noqa: BLE001
        raise ModelResolutionError(module_name, qualname) from exc
    target: Any = module
    for attribute in qualname.split("."):
        target = getattr(target, attribute, None)
        if target is None:
            raise ModelResolutionError(module_name, qualname)
    if not isinstance(target, type) or not issubclass(target, BaseModel):
        raise ModelResolutionError(module_name, qualname)
    return cast(type[BaseModel], target)
