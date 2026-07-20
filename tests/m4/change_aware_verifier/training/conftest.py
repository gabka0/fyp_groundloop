from __future__ import annotations

import sys
from pathlib import Path

TRAINING_ROOT = Path(__file__).resolve().parents[4] / "training"
sys.path.insert(0, str(TRAINING_ROOT))
