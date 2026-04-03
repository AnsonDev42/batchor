from __future__ import annotations

from enum import StrEnum


class ItemStatus(StrEnum):
    PENDING = "pending"
    QUEUED_LOCAL = "queued_local"
    SUBMITTED = "submitted"
    COMPLETED = "completed"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_PERMANENT = "failed_permanent"


class RunLifecycleStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_FAILURES = "completed_with_failures"


class ProviderKind(StrEnum):
    OPENAI = "openai"


class StorageKind(StrEnum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"
    MEMORY = "memory"


class OpenAIEndpoint(StrEnum):
    RESPONSES = "/v1/responses"
    CHAT_COMPLETIONS = "/v1/chat/completions"


class OpenAIModel(StrEnum):
    GPT_5_2 = "gpt-5.2"
    GPT_5_1 = "gpt-5.1"
    GPT_5 = "gpt-5"
    GPT_5_MINI = "gpt-5-mini"
    GPT_5_NANO = "gpt-5-nano"
    GPT_4_1 = "gpt-4.1"
    GPT_4_1_MINI = "gpt-4.1-mini"
    GPT_4_1_NANO = "gpt-4.1-nano"


class OpenAIReasoningEffort(StrEnum):
    NONE = "none"
    MINIMAL = "minimal"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    XHIGH = "xhigh"
