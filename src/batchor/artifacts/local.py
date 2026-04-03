from __future__ import annotations

from contextlib import AbstractContextManager
import os
from pathlib import Path
import shutil

from batchor.artifacts.base import ArtifactStore


class _LocalArtifactStage(AbstractContextManager[Path]):
    def __init__(self, path: Path) -> None:
        self._path = path

    def __enter__(self) -> Path:
        return self._path

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        return None


class LocalArtifactStore(ArtifactStore):
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._restrict_permissions(self.root, is_dir=True)

    def write_text(self, key: str, content: str, *, encoding: str = "utf-8") -> None:
        path = self.resolve_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._restrict_permissions(path.parent, is_dir=True)
        path.write_text(content, encoding=encoding)
        self._restrict_permissions(path, is_dir=False)

    def read_text(self, key: str, *, encoding: str = "utf-8") -> str:
        return self.resolve_path(key).read_text(encoding=encoding)

    def delete(self, key: str) -> bool:
        path = self.resolve_path(key)
        if not path.exists():
            return False
        if not path.is_file():
            raise IsADirectoryError(f"artifact path is not a file: {key}")
        path.unlink()
        self._prune_empty_parent_dirs(path.parent)
        return True

    def stage_local_copy(self, key: str) -> AbstractContextManager[Path]:
        return _LocalArtifactStage(self.resolve_path(key))

    def export_to_directory(self, key: str, destination_root: str | Path) -> Path:
        source_path = self.resolve_path(key)
        target_path = Path(destination_root).expanduser().resolve() / self._validated_relative_path(key)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
        return target_path

    def resolve_path(self, key: str) -> Path:
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
