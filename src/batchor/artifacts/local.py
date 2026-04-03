"""Local filesystem implementation of :class:`~batchor.ArtifactStore`.

Artifacts are stored under a root directory with permissions restricted to the
current user (``0o700`` for directories, ``0o600`` for files).  Empty parent
directories are pruned automatically when artifacts are deleted.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
import os
from pathlib import Path
import shutil

from batchor.artifacts.base import ArtifactStore


class _LocalArtifactStage(AbstractContextManager[Path]):
    """Trivial context manager that yields the artifact path directly.

    For a local store, no temporary copy is needed — the resolved path is
    returned as-is.
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    def __enter__(self) -> Path:
        return self._path

    def __exit__(self, exc_type, exc, exc_tb) -> None:  # noqa: ANN001
        return None


class LocalArtifactStore(ArtifactStore):
    """File-backed artifact store rooted at a single directory.

    The store is created with restrictive permissions (``0o700``) on first use.
    Key validation prevents path traversal: keys must be relative and must not
    contain ``..`` components.

    Attributes:
        root: Absolute :class:`~pathlib.Path` to the root directory.
    """

    def __init__(self, root: str | Path) -> None:
        """Initialise the store, creating the root directory if necessary.

        Args:
            root: Path to the root directory.  ``~`` is expanded and the path
                is resolved to an absolute form.
        """
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._restrict_permissions(self.root, is_dir=True)

    def write_text(self, key: str, content: str, *, encoding: str = "utf-8") -> None:
        """Write text content to a file at ``root / key``.

        Parent directories are created automatically.

        Args:
            key: Relative artifact key (must not be absolute or contain ``..``).
            content: Text to write.
            encoding: File encoding.  Defaults to ``"utf-8"``.

        Raises:
            ValueError: If *key* is absolute, empty, or traverses outside the root.
        """
        path = self.resolve_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._restrict_permissions(path.parent, is_dir=True)
        path.write_text(content, encoding=encoding)
        self._restrict_permissions(path, is_dir=False)

    def read_text(self, key: str, *, encoding: str = "utf-8") -> str:
        """Read text content from ``root / key``.

        Args:
            key: Relative artifact key.
            encoding: File encoding.  Defaults to ``"utf-8"``.

        Returns:
            The file's text contents.

        Raises:
            ValueError: If *key* is invalid.
            FileNotFoundError: If the artifact does not exist.
        """
        return self.resolve_path(key).read_text(encoding=encoding)

    def delete(self, key: str) -> bool:
        """Delete the artifact file at ``root / key``.

        Empty parent directories up to (but not including) the root are
        removed after deletion.

        Args:
            key: Relative artifact key.

        Returns:
            ``True`` if the file was found and deleted; ``False`` if not found.

        Raises:
            IsADirectoryError: If the resolved path is a directory.
            ValueError: If *key* is invalid.
        """
        path = self.resolve_path(key)
        if not path.exists():
            return False
        if not path.is_file():
            raise IsADirectoryError(f"artifact path is not a file: {key}")
        path.unlink()
        self._prune_empty_parent_dirs(path.parent)
        return True

    def stage_local_copy(self, key: str) -> AbstractContextManager[Path]:
        """Return a context manager yielding the resolved local path directly.

        Because this is a local store, no copy is needed; the artifact's
        actual path is returned inside the context.

        Args:
            key: Relative artifact key.

        Returns:
            A context manager that yields the local :class:`~pathlib.Path`.
        """
        return _LocalArtifactStage(self.resolve_path(key))

    def export_to_directory(self, key: str, destination_root: str | Path) -> Path:
        """Copy an artifact to ``destination_root / key``.

        Args:
            key: Relative artifact key identifying the source file.
            destination_root: Target directory; the artifact is written to
                ``destination_root / key`` with parent directories created as
                needed.

        Returns:
            Absolute path to the copied file.
        """
        source_path = self.resolve_path(key)
        target_path = Path(destination_root).expanduser().resolve() / self._validated_relative_path(key)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
        return target_path

    def resolve_path(self, key: str) -> Path:
        """Resolve an artifact key to an absolute filesystem path.

        Args:
            key: Relative artifact key.

        Returns:
            Absolute :class:`~pathlib.Path` under the store root.

        Raises:
            ValueError: If *key* is invalid.
        """
        return self.root / self._validated_relative_path(key)

    @staticmethod
    def _validated_relative_path(key: str) -> Path:
        relative_path = Path(key)
        if relative_path.is_absolute():
            raise ValueError(f"artifact key must be relative: {key}")
        if str(relative_path) in {"", "."}:
            raise ValueError("artifact key must not be empty")
        if ".." in relative_path.parts:
            raise ValueError(f"artifact key must not escape root: {key}")
        return relative_path

    def _prune_empty_parent_dirs(self, start: Path) -> None:
        directories = [start, *start.parents]
        for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
            if directory == self.root:
                break
            try:
                directory.rmdir()
            except OSError:
                continue

    @staticmethod
    def _restrict_permissions(path: Path, *, is_dir: bool) -> None:
        mode = 0o700 if is_dir else 0o600
        try:
            os.chmod(path, mode)
        except OSError:
            return
