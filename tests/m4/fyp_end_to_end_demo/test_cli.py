from __future__ import annotations

from pathlib import Path

import pytest

import groundloop.cli as cli
import groundloop.fyp_end_to_end_demo as demo_module


def test_fyp_e2e_parser_defaults_to_deterministic() -> None:
    args = cli.build_parser().parse_args(("fyp-e2e-demo",))

    assert args.backend == "deterministic"
    assert args.keep_schema is False
    assert args.output == Path("artifacts/fyp-e2e-demo/result.json")
    assert not hasattr(args, "allow_model_download")


def test_fyp_e2e_cli_delegates_writes_and_prints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sentinel = object()
    captured: dict[str, object] = {}

    def run(config: demo_module.FypEndToEndDemoConfig) -> object:
        captured["config"] = config
        return sentinel

    def write(result: object, output: Path) -> None:
        captured["write"] = (result, output)

    def format_summary(result: object, *, output: Path) -> str:
        captured["format"] = (result, output)
        return "bounded summary"

    monkeypatch.setattr(demo_module, "run_fyp_end_to_end_demo", run)
    monkeypatch.setattr(demo_module, "write_fyp_end_to_end_result", write)
    monkeypatch.setattr(demo_module, "format_fyp_end_to_end_summary", format_summary)
    output = tmp_path / "result.json"
    result = cli.main(
        (
            "fyp-e2e-demo",
            "--database-url",
            "postgresql+psycopg://db/groundloop",
            "--repo-root",
            str(tmp_path / "repo"),
            "--artifact-root",
            str(tmp_path / "artifacts"),
            "--backend",
            "real",
            "--model-config",
            str(tmp_path / "models.json"),
            "--lexical-config",
            str(tmp_path / "lexical.json"),
            "--output",
            str(output),
            "--keep-schema",
        )
    )

    assert result == 0
    config = captured["config"]
    assert isinstance(config, demo_module.FypEndToEndDemoConfig)
    assert config.database_url == "postgresql://db/groundloop"
    assert config.backend == "real"
    assert config.keep_schema is True
    assert captured["write"] == (sentinel, output)
    assert captured["format"] == (sentinel, output)
    assert capsys.readouterr().out == "bounded summary\n"
