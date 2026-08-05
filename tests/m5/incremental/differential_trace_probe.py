from __future__ import annotations

import json

from .test_overlay_differential import run_differential_trace_summary

if __name__ == "__main__":
    print(json.dumps(run_differential_trace_summary(), sort_keys=True))
