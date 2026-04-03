from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from pathlib import Path


class ArtifactStore(ABC):
    @abstractmethod
    def write_text(self, key: str, content: str, *, encoding: str = "utf-8") -> None: ...

    @abstractmethod
    def read_text(self, key: str, *, encoding: str = "utf-8") -> str: ...

    @abstractmethod
    def delete(self, key: str) -> bool: ...

    @abstractmethod
    def stage_local_copy(self, key: str) -> AbstractContextManager[Path]: ...

    @abstractmethod
    def export_to_directory(self, key: str, destination_root: str | Path) -> Path: ...
