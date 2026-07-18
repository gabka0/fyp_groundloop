"""Frozen identities and label order for the M3 verifier lane."""

from enum import IntEnum

BASE_MODEL_ID = "cross-encoder/nli-MiniLM2-L6-H768"
BASE_MODEL_REVISION = "b95119ce93d3e065de6214e38cd4a97b0f2f2c6d"
SCIFACT_DATASET_ID = "allenai/scifact"
SCIFACT_REVISION = "1fe54665deee011033b2dd98db5752e0d586fdfb"
WICE_REPOSITORY = "https://github.com/ryokamoi/wice.git"
WICE_REVISION = "ddeb6c183665e2a20c5f03c5aa07f03888b9870f"


class Label(IntEnum):
    """Stored/evaluation order, deliberately different from base-logit order."""

    SUPPORT = 0
    REFUTE = 1
    NEUTRAL = 2


LABELS = ("support", "refute", "neutral")
BASE_LOGIT_ORDER = ("contradiction", "entailment", "neutral")
BASE_TO_STORED = (Label.REFUTE, Label.SUPPORT, Label.NEUTRAL)
