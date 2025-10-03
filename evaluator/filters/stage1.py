# -*- coding: utf-8 -*-
"""
Stage-1 gate driven by model JSON.
Backwards compatible signature: stage1_pass(snapshot: dict, cfg: dict|None)
- If cfg is provided, it will be deep-merged into the loaded model config (as overrides).
"""

import os
from .filter_engine import ModelConfigLoader, FilterEngine, deep_merge
# Default to the first strategy you asked for
_DEFAULT_MODEL = os.getenv("REFLEX_MODEL", "bull_retrace_v1")

_loader = ModelConfigLoader()
_engine = FilterEngine()

def stage1_pass(snapshot: dict, cfg: dict | None = None, model: str = _DEFAULT_MODEL) -> bool:
    model_cfg = _loader.load(model)
    if cfg:
        model_cfg = deep_merge(model_cfg, cfg)
    stage_def = (model_cfg.get("stages") or {}).get("stage1", {})
    params = (model_cfg.get("entry") or {}).get("params", {})
    return _engine.evaluate(snapshot, stage_def, params=params)
