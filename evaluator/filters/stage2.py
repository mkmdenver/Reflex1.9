# -*- coding: utf-8 -*-
"""
Stage-2 gate driven by model JSON.
- Uses same engine; generally tighter intraday constraints than Stage-1.
"""

import os
from .filter_engine import ModelConfigLoader, FilterEngine, deep_merge

_DEFAULT_MODEL = os.getenv("REFLEX_MODEL", "bull_retrace_v1")

_loader = ModelConfigLoader()
_engine = FilterEngine()

def stage2_pass(snapshot: dict, cfg: dict | None = None, model: str = _DEFAULT_MODEL) -> bool:
    model_cfg = _loader.load(model)
    if cfg:
        model_cfg = deep_merge(model_cfg, cfg)
    stage_def = (model_cfg.get("stages") or {}).get("stage2", {})
    params = (model_cfg.get("entry") or {}).get("params", {})
    return _engine.evaluate(snapshot, stage_def, params=params)
