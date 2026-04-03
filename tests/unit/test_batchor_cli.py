from __future__ import annotations

import json
import os
from pathlib import Path

from pydantic import BaseModel
from typer.testing import CliRunner

from batchor.cli import create_app
from batchor.core.models import OpenAIProviderConfig, PromptParts
from batchor.providers.openai import OpenAIBatchProvider


class _FakeCliProvider:
    def __init__(self) -> None:
        self._parser = OpenAIBatchProvider(
            OpenAIProviderConfig(api_key="k", model="gpt-4.1"),
            client=object(),
        )
        self._current_lines: list[dict[str, object]] = []
        self._file_to_lines: dict[str, list[dict[str, object]]] = {}
        self._batch_to_file: dict[str, str] = {}
        self._next_file = 0
        self._next_batch = 0
        self.env_seen_during_build: str | None = None
        self.structured_mode = False

    def build_request_line(
        self,
        *,
        custom_id: str,
        prompt_parts: PromptParts,
        structured_output=None,  # noqa: ANN001
    ) -> dict[str, object]:
        self.env_seen_during_build = os.getenv("OPENAI_API_KEY")
        self.structured_mode = structured_output is not None
        return {
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/responses",
            "body": {"input": prompt_parts.prompt},
        }

    def upload_input_file(self, input_path: Path) -> str:
        self._current_lines = [
            json.loads(raw_line) for raw_line in input_path.read_text(encoding="utf-8").splitlines() if raw_line.strip()
        ]
        file_id = f"file_{self._next_file}"
        self._next_file += 1
        self._file_to_lines[file_id] = list(self._current_lines)
        return file_id

    def delete_input_file(self, file_id: str) -> None:
        self._file_to_lines.pop(file_id, None)

    def create_batch(self, *, input_file_id: str, metadata: dict[str, str] | None = None) -> dict[str, object]:
        batch_id = f"batch_{self._next_batch}"
        self._next_batch += 1
        self._batch_to_file[batch_id] = input_file_id
        return {"id": batch_id, "status": "submitted", "metadata": metadata or {}}

    def get_batch(self, batch_id: str) -> dict[str, object]:
        return {
            "id": batch_id,
            "status": "completed",
            "output_file_id": f"output_{batch_id}",
            "error_file_id": None,
        }

    def download_file_content(self, file_id: str) -> str:
        if not file_id.startswith("output_"):
            return ""
        batch_id = file_id.replace("output_", "")
        records = []
        for line in self._file_to_lines[self._batch_to_file[batch_id]]:
            text = f"ok:{line['custom_id']}"
            if self.structured_mode:
                text = '{"label":"ok","score":1.0}'
            records.append(
                json.dumps(
                    {
                        "custom_id": line["custom_id"],
                        "response": {
                            "status_code": 200,
                            "body": {"output": [{"content": [{"text": text}]}]},
                        },
                    }
                )
            )
        return "\n".join(records) + ("\n" if records else "")

    def parse_batch_output(self, *, output_content: str | None, error_content: str | None):
        return self._parser.parse_batch_output(
            output_content=output_content,
            error_content=error_content,
        )

    def estimate_request_tokens(
        self,
        request_line: dict[str, object],
        *,
        chars_per_token: int,
    ) -> int:
        del chars_per_token
        return max(len(json.dumps(request_line)), 1)


class _CliStructuredResult(BaseModel):
    label: str
    score: float


def test_cli_start_loads_dotenv_for_local_usage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    input_path = tmp_path / "items.jsonl"
    input_path.write_text('{"id":"row1","text":"hello"}\n', encoding="utf-8")
    (tmp_path / ".env").write_text("OPENAI_API_KEY=dotenv-key\n", encoding="utf-8")
    provider = _FakeCliProvider()
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    result = runner.invoke(
        create_app(provider_factory=lambda _cfg: provider),
        [
            "start",
            "--input",
            str(input_path),
            "--id-field",
            "id",
            "--prompt-field",
            "text",
            "--model",
            "gpt-4.1",
            "--db-path",
            str(tmp_path / "cli.sqlite3"),
        ],
    )

    assert result.exit_code == 0
    assert provider.env_seen_during_build == "dotenv-key"
    payload = json.loads(result.stdout)
    assert payload["run_id"].startswith("batchor_")


def test_cli_wait_and_results_cover_operator_flow(tmp_path: Path) -> None:
    input_path = tmp_path / "items.csv"
    input_path.write_text("id,text\nrow1,hello\n", encoding="utf-8")
    db_path = tmp_path / "cli.sqlite3"
    output_path = tmp_path / "results.jsonl"
    provider = _FakeCliProvider()
    runner = CliRunner()
    app = create_app(provider_factory=lambda _cfg: provider)

    start = runner.invoke(
        app,
        [
            "start",
            "--input",
            str(input_path),
            "--id-field",
            "id",
            "--prompt-template",
            "Prompt: {text}",
            "--model",
            "gpt-4.1",
            "--db-path",
            str(db_path),
        ],
    )
    assert start.exit_code == 0
    run_id = json.loads(start.stdout)["run_id"]

    wait = runner.invoke(
        app,
        ["wait", "--run-id", run_id, "--db-path", str(db_path), "--poll-interval", "0"],
    )
    assert wait.exit_code == 0
    assert json.loads(wait.stdout)["status"] == "completed"

    results = runner.invoke(
        app,
        ["results", "--run-id", run_id, "--db-path", str(db_path), "--output", str(output_path)],
    )
    assert results.exit_code == 0
    written = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(written) == 1
    assert json.loads(written[0])["output_text"] == "ok:row1:a1"


def test_cli_start_validates_required_prompt_fields(tmp_path: Path) -> None:
    input_path = tmp_path / "items.jsonl"
    input_path.write_text('{"id":"row1","text":"hello"}\n', encoding="utf-8")
    provider = _FakeCliProvider()
    runner = CliRunner()

    result = runner.invoke(
        create_app(provider_factory=lambda _cfg: provider),
        [
            "start",
            "--input",
            str(input_path),
            "--id-field",
            "id",
            "--prompt-template",
            "Prompt: {missing}",
            "--model",
            "gpt-4.1",
            "--db-path",
            str(tmp_path / "cli.sqlite3"),
        ],
    )

    assert result.exit_code == 1
    assert "missing required fields: missing" in result.stderr


def test_cli_structured_output_supports_importable_model(tmp_path: Path) -> None:
    input_path = tmp_path / "items.jsonl"
    input_path.write_text('{"id":"row1","text":"hello"}\n', encoding="utf-8")
    db_path = tmp_path / "cli-structured.sqlite3"
    output_path = tmp_path / "structured-results.jsonl"
    provider = _FakeCliProvider()
    runner = CliRunner()
    app = create_app(provider_factory=lambda _cfg: provider)

    start = runner.invoke(
        app,
        [
            "start",
            "--input",
            str(input_path),
            "--id-field",
            "id",
            "--prompt-field",
            "text",
            "--structured-output-class",
            f"{__name__}:_CliStructuredResult",
            "--schema-name",
            "cli_structured_result",
            "--model",
            "gpt-4.1",
            "--db-path",
            str(db_path),
        ],
    )
    assert start.exit_code == 0
    run_id = json.loads(start.stdout)["run_id"]

    wait = runner.invoke(
        app,
        ["wait", "--run-id", run_id, "--db-path", str(db_path), "--poll-interval", "0"],
    )
    assert wait.exit_code == 0

    results = runner.invoke(
        app,
        ["results", "--run-id", run_id, "--db-path", str(db_path), "--output", str(output_path)],
    )
    assert results.exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8").strip())
    assert payload["output"]["label"] == "ok"


def test_cli_start_supports_multiple_inputs_with_duplicate_ids(tmp_path: Path) -> None:
    csv_path = tmp_path / "items.csv"
    jsonl_path = tmp_path / "items.jsonl"
    db_path = tmp_path / "cli-multi.sqlite3"
    output_path = tmp_path / "multi-results.jsonl"
    csv_path.write_text("id,text\nrow1,hello\n", encoding="utf-8")
    jsonl_path.write_text('{"id":"row1","text":"world"}\n', encoding="utf-8")
    provider = _FakeCliProvider()
    runner = CliRunner()
    app = create_app(provider_factory=lambda _cfg: provider)

    start = runner.invoke(
        app,
        [
            "start",
            "--input",
            str(csv_path),
            "--input",
            str(jsonl_path),
            "--id-field",
            "id",
            "--prompt-field",
            "text",
            "--model",
            "gpt-4.1",
            "--db-path",
            str(db_path),
        ],
    )
    assert start.exit_code == 0
    run_id = json.loads(start.stdout)["run_id"]

    wait = runner.invoke(
        app,
        ["wait", "--run-id", run_id, "--db-path", str(db_path), "--poll-interval", "0"],
    )
    assert wait.exit_code == 0

    results = runner.invoke(
        app,
        ["results", "--run-id", run_id, "--db-path", str(db_path), "--output", str(output_path)],
    )
    assert results.exit_code == 0
    written = [json.loads(line) for line in output_path.read_text(encoding="utf-8").strip().splitlines()]
    first_namespace = written[0]["item_id"].split("__", maxsplit=1)[0]
    second_namespace = written[1]["item_id"].split("__", maxsplit=1)[0]

    assert [entry["item_id"] for entry in written] == [
        f"{first_namespace}__row1",
        f"{second_namespace}__row1",
    ]
    assert first_namespace.startswith("src_")
    assert second_namespace.startswith("src_")
    assert first_namespace != second_namespace
    assert [entry["metadata"]["batchor_lineage"]["source_primary_key"] for entry in written] == [
        "row1",
        "row1",
    ]


def test_cli_start_rejects_unsupported_input_suffix_when_repeated(tmp_path: Path) -> None:
    csv_path = tmp_path / "items.csv"
    txt_path = tmp_path / "items.txt"
    csv_path.write_text("id,text\nrow1,hello\n", encoding="utf-8")
    txt_path.write_text("row1 hello\n", encoding="utf-8")
    provider = _FakeCliProvider()
    runner = CliRunner()

    result = runner.invoke(
        create_app(provider_factory=lambda _cfg: provider),
        [
            "start",
            "--input",
            str(csv_path),
            "--input",
            str(txt_path),
            "--id-field",
            "id",
            "--prompt-field",
            "text",
            "--model",
            "gpt-4.1",
            "--db-path",
            str(tmp_path / "cli.sqlite3"),
        ],
    )

    assert result.exit_code == 1
    assert "input file must end with .csv or .jsonl" in result.stderr
