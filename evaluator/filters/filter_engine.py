# -*- coding: utf-8 -*-
"""
Generic filter engine for model-driven Stage-1/Stage-2 gates.

- Loads a single model JSON (per strategy) with flexible, declarative filters.
- Evaluates snapshot fields with operators: ==, !=, >, >=, <, <=, between, in, not_in, exists, missing.
- Supports field-to-field comparisons, value_from (pulling from params), and simple transforms (abs).
- Combines rules with logic "all"/"any" and optional "any" adjunct list for additional flexibility.

Author: Reflex
"""

from __future__ import annotations
import json
import os
from typing import Any, Dict, List, Optional, Tuple, Union
from threading import RLock

Number = Union[int, float]

# ---------- Utilities ----------
def _deep_get(d: dict, path: str, default: Any = None) -> Any:
    """Get nested key with 'a.b.c' syntax."""
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def _apply_transform(val: Any, transform: Optional[str]) -> Any:
    if transform is None:
        return val
    if transform == "abs":
        try:
            return abs(float(val))
        except Exception:
            return val
    return val  # extend with more transforms as needed
def _to_number(x: Any) -> Optional[Number]:
    try:
        return float(x)
    except Exception:
        return None

# ---------- Model loader with caching ----------
class ModelConfigLoader:
    """
    Loads model JSON from `models/` directory (by name) or an absolute path.
    Caches results to avoid repeated disk I/O.
    """
    def __init__(self, base_dir: Optional[str] = None):
        self.base_dir = base_dir or os.path.join(os.path.dirname(__file__), "..", "..", "models")
        self._cache: Dict[str, dict] = {}
        self._lock = RLock()

    def load(self, model_name_or_path: str) -> dict:
        key = os.path.abspath(model_name_or_path) if os.path.isabs(model_name_or_path) else model_name_or_path
        with self._lock:
            if key in self._cache:
                return self._cache[key]
        # Resolve path
        if os.path.isabs(model_name_or_path):
            path = model_name_or_path
        else:
            # e.g., "bull_retrace_v1" -> models/bull_retrace_v1.json
            path = os.path.join(self.base_dir, f"{model_name_or_path}.json")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Model JSON not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        with self._lock:
            self._cache[key] = data
        return data

# ---------- Core engine ----------
class FilterEngine:
    """
    Evaluates filter definitions against a 'snapshot' dict.

    Filter definition schema:
    {
      "logic": "all" | "any",
      "rules": [
        {
          "field": "rvol",
          "op": ">=",
          "value": 2.0,
          "value_from": "params.min_rvol",
          "field2": "vwap",
          "transform": "abs",
          "optional": true
        },
        ...
      ],
      "any": [ ... same rule objects ... ]   # optional adjunct OR-block
    }
    """

    def __init__(self):
        self._ops = {
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            ">":  lambda a, b: (a is not None and b is not None and a >  b),
            ">=": lambda a, b: (a is not None and b is not None and a >= b),
            "<":  lambda a, b: (a is not None and b is not None and a <  b),
            "<=": lambda a, b: (a is not None and b is not None and a <= b),
            "between": self._between,
            "in":      lambda a, b: a in (b or []),
            "not_in":  lambda a, b: a not in (b or []),
            "exists":  lambda a, _: a is not None,
            "missing": lambda a, _: a is None,
        }

    @staticmethod
    def _between(a: Any, bounds: List[Number]) -> bool:
        if a is None or not isinstance(bounds, (list, tuple)) or len(bounds) != 2:
            return False
        lo, hi = bounds
        na = _to_number(a)
        return (na is not None) and (na >= lo) and (na <= hi)

    def evaluate(self, snapshot: dict, defn: dict, params: Optional[dict] = None) -> bool:
        params = params or {}
        logic = (defn or {}).get("logic", "all").lower()
        rules = (defn or {}).get("rules", [])
        any_rules = (defn or {}).get("any", [])

        def eval_rule(rule: dict) -> Optional[bool]:
            # Resolve left value
            field = rule.get("field")
            a = _deep_get(snapshot, field, None) if field else None
            a = _apply_transform(a, rule.get("transform"))

            # Resolve the comparator (b)
            if "field2" in rule:
                b = _apply_transform(_deep_get(snapshot, rule["field2"], None), rule.get("transform"))
            elif "value_from" in rule:
                b = _deep_get({"params": params}, rule["value_from"], None)
            else:
                b = rule.get("value", None)

            op = rule.get("op", "exists")
            fn = self._ops.get(op)
            if not fn:
                return None  # undefined operator

            try:
                # numeric-safe compare where useful
                if op in (">", ">=", "<", "<=", "between"):
                    a_num = _to_number(a)
                    if op == "between":
                        return fn(a_num, b)
                    b_num = _to_number(b)
                    return fn(a_num, b_num)
                return fn(a, b)
            except Exception:
                return False

        # Main block
        results: List[bool] = []
        for r in rules:
            out = eval_rule(r)
            if out is None:
                if not r.get("optional"):
                    results.append(False)
            else:
                results.append(out)

        passed = all(results) if logic == "all" else any(results) if results else False

        # Optional adjunct OR-block
        if not passed and any_rules:
            adj = []
            for r in any_rules:
                out = eval_rule(r)
                if out is None:
                    if not r.get("optional"):
                        adj.append(False)
                else:
                    adj.append(out)
            passed = any(adj) if adj else passed

        return passed

# ---------- Tiny merge helper for overrides ----------
def deep_merge(a: dict, b: dict) -> dict:
    """Return a deep-merged dict of a + b (b overwrites)."""
    out = dict(a or {})
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out
