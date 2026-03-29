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


class ProviderKind(StrEnum):
    OPENAI = "openai"


class StorageKind(StrEnum):
    SQLITE = "sqlite"
    MEMORY = "memory"


class OpenAIEndpoint(StrEnum):
    RESPONSES = "/v1/responses"
    CHAT_COMPLETIONS = "/v1/chat/completions"
