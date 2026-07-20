from pathlib import Path

import pytest

from groundloop.errors import ValidationError
from groundloop.m4.real_dynamic_history import M4RealDynamicHistoryConfig
from groundloop.m4.smoke import SmokeUnavailableError


def test_history_config_rejects_missing_database_url() -> None:
    with pytest.raises(SmokeUnavailableError, match="database URL is absent"):
        M4RealDynamicHistoryConfig("", Path("."), Path("."))


def test_history_config_rejects_unsafe_schema_prefix() -> None:
    with pytest.raises(ValidationError, match="schema prefix"):
        M4RealDynamicHistoryConfig(
            "postgresql://fixture", Path("."), Path("."), schema_prefix="bad-name"
        )
