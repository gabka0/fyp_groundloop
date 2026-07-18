"""Conflict-detecting in-memory registries for immutable M3 artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field

from groundloop.ai.contracts import ModelArtifact, PromptArtifact
from groundloop.errors import ArtifactConflictError


@dataclass(slots=True)
class InMemoryModelRegistry:
    _artifacts: dict[str, ModelArtifact] = field(default_factory=dict)

    def register(self, artifact: ModelArtifact) -> bool:
        """Register an artifact; return True iff it was newly inserted."""
        existing = self._artifacts.get(artifact.artifact_id)
        if existing is None:
            self._artifacts[artifact.artifact_id] = artifact
            return True
        if existing != artifact:
            raise ArtifactConflictError(
                f"model artifact {artifact.artifact_id} has conflicting content"
            )
        return False

    def get(self, artifact_id: str) -> ModelArtifact:
        return self._artifacts[artifact_id]

    def all(self) -> tuple[ModelArtifact, ...]:
        return tuple(self._artifacts[key] for key in sorted(self._artifacts))


@dataclass(slots=True)
class InMemoryPromptRegistry:
    _artifacts: dict[str, PromptArtifact] = field(default_factory=dict)

    def register(self, artifact: PromptArtifact) -> bool:
        """Register an artifact; return True iff it was newly inserted."""
        existing = self._artifacts.get(artifact.artifact_id)
        if existing is None:
            self._artifacts[artifact.artifact_id] = artifact
            return True
        if existing != artifact:
            raise ArtifactConflictError(
                f"prompt artifact {artifact.artifact_id} has conflicting content"
            )
        return False

    def get(self, artifact_id: str) -> PromptArtifact:
        return self._artifacts[artifact_id]

    def all(self) -> tuple[PromptArtifact, ...]:
        return tuple(self._artifacts[key] for key in sorted(self._artifacts))
