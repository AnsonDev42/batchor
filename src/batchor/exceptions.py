from __future__ import annotations


class RunNotFinishedError(RuntimeError):
    def __init__(self, run_id: str) -> None:
        super().__init__(f"run {run_id} is not finished")
        self.run_id = run_id


class ModelResolutionError(RuntimeError):
    def __init__(self, module_name: str, qualname: str) -> None:
        super().__init__(
            "structured output model unavailable for rehydration: "
            f"{module_name}:{qualname}"
        )
        self.module_name = module_name
        self.qualname = qualname
