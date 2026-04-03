"""Enumeration types shared across the batchor domain.

Defines stable string-valued enums for item lifecycle, run lifecycle, run
control signals, and provider/storage kind identifiers.  Every enum uses
``StrEnum`` so values are directly serialisable to JSON without conversion.
"""

from __future__ import annotations

from enum import StrEnum


class ItemStatus(StrEnum):
    """Lifecycle status for a single batch item.

    Attributes:
        PENDING: Item is waiting to be claimed for submission.
        QUEUED_LOCAL: Item has been claimed locally but not yet submitted to
            the provider.
        SUBMITTED: Item has been included in an active provider batch.
        COMPLETED: Item reached a terminal success state.
        FAILED_RETRYABLE: Item failed on this attempt but will be retried.
        FAILED_PERMANENT: Item exceeded the maximum retry limit and will not
            be retried.
    """

    PENDING = "pending"
    QUEUED_LOCAL = "queued_local"
    SUBMITTED = "submitted"
    COMPLETED = "completed"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_PERMANENT = "failed_permanent"


class RunLifecycleStatus(StrEnum):
    """Terminal or active lifecycle status for a batch run.

    Attributes:
        RUNNING: Run is active and processing items.
        COMPLETED: All items completed successfully.
        COMPLETED_WITH_FAILURES: Run finished but at least one item reached
            ``FAILED_PERMANENT`` status.
    """

    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_FAILURES = "completed_with_failures"


class RunControlState(StrEnum):
    """Operator-controlled state used to pause, resume, or cancel a run.

    Attributes:
        RUNNING: Normal execution — the runner processes items.
        PAUSED: Execution is suspended; ``refresh()`` returns immediately
            without polling or submitting.
        CANCEL_REQUESTED: Cancellation has been requested; the runner drains
            in-flight batches and marks remaining items as cancelled.
    """

    RUNNING = "running"
    PAUSED = "paused"
    CANCEL_REQUESTED = "cancel_requested"


class ProviderKind(StrEnum):
    """Stable identifier for a batch execution provider.

    Attributes:
        OPENAI: The built-in OpenAI Batch API provider.
    """

    OPENAI = "openai"


class StorageKind(StrEnum):
    """Identifier for a state-store backend.

    Attributes:
        SQLITE: File-backed SQLite store (default, zero-dependency).
        POSTGRES: Shared PostgreSQL control-plane store.
        MEMORY: Ephemeral in-memory store (testing / single-process use).
    """

    SQLITE = "sqlite"
    POSTGRES = "postgres"
    MEMORY = "memory"


class OpenAIEndpoint(StrEnum):
    """OpenAI API endpoint path used in Batch request lines.

    Attributes:
        RESPONSES: The ``/v1/responses`` endpoint (default).
        CHAT_COMPLETIONS: The ``/v1/chat/completions`` endpoint.
    """

    RESPONSES = "/v1/responses"
    CHAT_COMPLETIONS = "/v1/chat/completions"


class OpenAIModel(StrEnum):
    """Known OpenAI model identifiers.

    Use these constants or pass a plain string to
    :attr:`~batchor.OpenAIProviderConfig.model` for forward-compatibility with
    models released after this version.

    Attributes:
        GPT_5_2: ``gpt-5.2``
        GPT_5_1: ``gpt-5.1``
        GPT_5: ``gpt-5``
        GPT_5_MINI: ``gpt-5-mini``
        GPT_5_NANO: ``gpt-5-nano``
        GPT_4_1: ``gpt-4.1``
        GPT_4_1_MINI: ``gpt-4.1-mini``
        GPT_4_1_NANO: ``gpt-4.1-nano``
    """

    GPT_5_2 = "gpt-5.2"
    GPT_5_1 = "gpt-5.1"
    GPT_5 = "gpt-5"
    GPT_5_MINI = "gpt-5-mini"
    GPT_5_NANO = "gpt-5-nano"
    GPT_4_1 = "gpt-4.1"
    GPT_4_1_MINI = "gpt-4.1-mini"
    GPT_4_1_NANO = "gpt-4.1-nano"


class OpenAIReasoningEffort(StrEnum):
    """Reasoning effort level passed to supporting OpenAI models.

    Attributes:
        NONE: Disable extended reasoning.
        MINIMAL: Minimal reasoning tokens.
        LOW: Low reasoning effort.
        MEDIUM: Medium reasoning effort (balanced).
        HIGH: High reasoning effort.
        XHIGH: Maximum reasoning effort.
    """

    NONE = "none"
    MINIMAL = "minimal"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    XHIGH = "xhigh"
