from __future__ import annotations

from pathlib import Path

import pytest

import groundloop.cli as cli
import groundloop.fyp_impact_pareto as pareto_module


def test_impact_pareto_parser_defaults(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    args = cli.build_parser().parse_args(("fyp-impact-pareto", "--source", str(source)))

    assert args.config == Path("configs/fyp/impact_pareto_v1.json")
    assert args.hosted_config == Path(
        "configs/fyp/hosted_verifier_openai_luna_v1.json"
    )
    assert args.source == source
    assert args.output_dir == Path("artifacts/fyp-impact-pareto")


def test_impact_pareto_cli_delegates_without_network(
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
        return "offline Pareto summary"

    monkeypatch.setattr(pareto_module, "execute_impact_pareto", execute)
    monkeypatch.setattr(pareto_module, "format_impact_pareto_summary", summary)
    config = tmp_path / "config.json"
    source = tmp_path / "source.jsonl"
    hosted = tmp_path / "hosted.json"
    output = tmp_path / "output"

    assert (
        cli.main(
            (
                "fyp-impact-pareto",
                "--config",
                str(config),
                "--source",
                str(source),
                "--hosted-config",
                str(hosted),
                "--output-dir",
                str(output),
            )
        )
        == 0
    )
    assert captured["execute"] == {
        "config_path": config,
        "source_path": source,
        "hosted_config_path": hosted,
        "output_directory": output,
    }
    assert captured["summary"] == (result, output)
    assert capsys.readouterr().out == "offline Pareto summary\n"
