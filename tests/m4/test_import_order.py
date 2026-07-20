from __future__ import annotations

import subprocess
import sys


def test_models_can_be_imported_before_admission_manifest() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from groundloop.m4.models.config import PinnedM3ReuseConfig; "
                "from groundloop.m4.admission.manifest import "
                "build_candidate_policy_manifest"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
