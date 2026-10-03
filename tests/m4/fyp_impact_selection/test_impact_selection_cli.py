from __future__ import annotations

from pathlib import Path

import pytest

import groundloop.cli as cli
import groundloop.fyp_impact_selection as selection_module


def test_impact_selection_parser_defaults(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    args = cli.build_parser().parse_args(
        ("fyp-impact-selection", "--source", str(source))
    )

    assert args.config == Path("configs/fyp/impact_selection_v1.json")
    assert args.source == source
    assert args.output_dir == Path("artifacts/fyp-impact-selection")


def test_impact_selection_cli_delegates_and_prints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}
    result = object()

    def execute(**kwargs: object) -> tuple[object, object]:
        captured["execute"] = kwargs
        return result, object()

    def summary(value: object, *, output_directory: Path) -> str:
        captured["summary"] = (value, output_directory)
        return "bounded impact summary"

    monkeypatch.setattr(selection_module, "execute_impact_selection", execute)
    monkeypatch.setattr(selection_module, "format_impact_selection_summary", summary)
    config = tmp_path / "config.json"
    source = tmp_path / "source.jsonl"
    output = tmp_path / "output"

    assert (
        cli.main(
            (
                "fyp-impact-selection",
                "--config",
                str(config),
                "--source",
                str(source),
                "--output-dir",
                str(output),
            )
        )
        == 0
    )
    assert captured["execute"] == {
        "config_path": config,
        "source_path": source,
        "output_directory": output,
    }
    assert captured["summary"] == (result, output)
    assert capsys.readouterr().out == "bounded impact summary\n"
