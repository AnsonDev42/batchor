"""Domain exceptions raised by the batchor runtime.

All public exceptions inherit from standard library base classes so callers
can catch them without importing from this module.
"""

from __future__ import annotations


class RunNotFinishedError(RuntimeError):
    """Raised when a terminal-only operation is called on a still-running run.

    Attributes:
        run_id: The identifier of the run that is not yet finished.
    """

    def __init__(self, run_id: str) -> None:
        """Initialise the error with the run identifier.

        Args:
            run_id: Identifier of the run that is not yet in a terminal state.
        """
        super().__init__(f"run {run_id} is not finished")
        self.run_id = run_id


class ModelResolutionError(RuntimeError):
    """Raised when a structured-output model class cannot be re-imported.

    This happens when a run is resumed in a process that does not have the
    same Python environment or import path as the process that created it.

    Attributes:
        module_name: The ``__module__`` of the model class.
        qualname: The ``__qualname__`` of the model class.
    """

    def __init__(self, module_name: str, qualname: str) -> None:
        """Initialise the error with the unresolvable model location.

        Args:
            module_name: Module path stored for the structured output class.
            qualname: Qualified name stored for the structured output class.
        """
        super().__init__(
            "structured output model unavailable for rehydration: "
            f"{module_name}:{qualname}"
        )
        self.module_name = module_name
        self.qualname = qualname


class RunPausedError(RuntimeError):
    """Raised when :meth:`~batchor.Run.wait` encounters a paused run.

    Attributes:
        run_id: The identifier of the paused run.
    """

    def __init__(self, run_id: str) -> None:
        """Initialise the error with the paused run's identifier.

        Args:
            run_id: Identifier of the run that is in a paused control state.
        """
        super().__init__(f"run {run_id} is paused")
        self.run_id = run_id


class StructuredOutputSchemaError(ValueError):
    """Raised when a Pydantic model produces a schema incompatible with OpenAI.

    OpenAI structured output requires strict JSON Schema objects with no
    ``anyOf`` at the root and ``additionalProperties: false`` on every object
    type.  This error is raised during job construction so invalid schemas are
    caught before any API calls are made.
    """
