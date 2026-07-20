"""M4.13 change-aware verifier preparation, training, and calibration utilities.

The package is deliberately an explicit-command surface.  Importing it never
loads model weights, downloads data, or opens the sealed terminal reserve.
"""

from .losses import (
    BASE_LOGIT_ORDER,
    STORED_LABEL_ORDER,
    LossBreakdown,
    endpoint_cross_entropy,
    paired_revision_margin_loss,
    stored_label_to_base_index,
    vitaminc_label_to_stored,
)

__all__ = [
    "BASE_LOGIT_ORDER",
    "STORED_LABEL_ORDER",
    "LossBreakdown",
    "endpoint_cross_entropy",
    "paired_revision_margin_loss",
    "stored_label_to_base_index",
    "vitaminc_label_to_stored",
]
