"""Abstract base class for artifact storage backends.

An :class:`ArtifactStore` handles the durable storage of raw provider output
files (request JSONL, output JSONL, error JSONL) generated during batch runs.
The default implementation is :class:`~batchor.LocalArtifactStore`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from pathlib import Path


class ArtifactStore(ABC):
    """Abstract interface for reading, writing, and deleting run artifacts.

    All paths are *relative keys* — the store implementation is responsible
    for mapping them to a concrete location (e.g. a local filesystem subtree
    or an object-storage bucket).
    """

    @abstractmethod
    def write_text(self, key: str, content: str, *, encoding: str = "utf-8") -> None:
        """Write text content to the artifact store under the given key.

        Args:
            key: Relative path identifying the artifact.
            content: Text content to write.
            encoding: Text encoding.  Defaults to ``"utf-8"``.
        """
        ...

    @abstractmethod
    def read_text(self, key: str, *, encoding: str = "utf-8") -> str:
        """Read text content for the given key.

        Args:
            key: Relative path identifying the artifact.
            encoding: Text encoding.  Defaults to ``"utf-8"``.

        Returns:
            The stored text content.
        """
        ...

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete the artifact at the given key.

        Args:
            key: Relative path identifying the artifact.

        Returns:
            ``True`` if the artifact existed and was deleted; ``False`` if it
            was not found.
        """
        ...

    @abstractmethod
    def stage_local_copy(self, key: str) -> AbstractContextManager[Path]:
        """Return a context manager that exposes a local filesystem path.

        Inside the context, the returned :class:`~pathlib.Path` is guaranteed
        to point to a readable copy of the artifact.  The caller must not
        modify the file.

        Args:
            key: Relative path identifying the artifact.

        Returns:
            A context manager yielding a :class:`~pathlib.Path` to a local
            copy of the artifact.
        """
        ...

    @abstractmethod
    def export_to_directory(self, key: str, destination_root: str | Path) -> Path:
        """Copy an artifact to a local directory, mirroring the key structure.

        Args:
            key: Relative path identifying the artifact.
            destination_root: Target directory.  The artifact is written to
                ``destination_root / key``, creating parent directories as
                needed.

        Returns:
            The absolute :class:`~pathlib.Path` where the artifact was written.
        """
        ...
