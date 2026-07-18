"""Explicit domain exceptions for GroundLoop.

Inconsistent input is rejected, never silently repaired.
"""


class GroundLoopError(Exception):
    """Base class for all GroundLoop domain errors."""


class ValidationError(GroundLoopError):
    """A record violates a domain invariant (scores, thresholds, shapes)."""


class DuplicateIdentifierError(GroundLoopError):
    """A stable identifier was registered more than once."""


class DanglingReferenceError(GroundLoopError):
    """A record references an identifier that does not exist."""


class EventConflictError(GroundLoopError):
    """An event identifier was reused with a different payload."""


class InvalidEventError(GroundLoopError):
    """An event is structurally invalid for the current repository state."""
