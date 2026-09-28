from __future__ import annotations

from pathlib import Path

import pytest

import groundloop.cli as cli
import groundloop.fyp_value_benchmark as benchmark_module


def test_fyp_value_benchmark_parser_defaults() -> None:
    args = cli.build_parser().parse_args(("fyp-value-benchmark",))

    assert args.config == Path("configs/m4/evaluation/controlled_v1.json")
    assert args.output_dir == Path("artifacts/fyp-value-benchmark")


def test_fyp_value_benchmark_cli_delegates_and_prints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sentinel_result = object()
    sentinel_bundle = object()
    captured: dict[str, object] = {}

    def execute(*, config_path: Path, output_directory: Path) -> tuple[object, object]:
        captured["execute"] = (config_path, output_directory)
        return sentinel_result, sentinel_bundle

    def format_summary(result: object, *, output_directory: Path) -> str:
        captured["format"] = (result, output_directory)
        return "bounded value summary"

    monkeypatch.setattr(benchmark_module, "execute_fyp_value_benchmark", execute)
    monkeypatch.setattr(
        benchmark_module, "format_fyp_value_benchmark_summary", format_summary
    )
    config = tmp_path / "config.json"
    output = tmp_path / "output"

    assert (
        cli.main(
            (
                "fyp-value-benchmark",
                "--config",
                str(config),
                "--output-dir",
                str(output),
            )
        )
        == 0
    )
    assert captured["execute"] == (config, output)
    assert captured["format"] == (sentinel_result, output)
    assert capsys.readouterr().out == "bounded value summary\n"
