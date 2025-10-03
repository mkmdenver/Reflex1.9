import os, json, threading, time
from collections import deque, defaultdict
from common.logger import setup_logger
from common.timeutils import utcnow
from pubsub.bus import BUS
from datahub.registry import REGISTRY

log = setup_logger('evaluator')

# --- Load JSON model ---------------------------------------------------------
HERE = os.path.dirname(__file__)
MODELS_DIR = os.path.normpath(os.path.join(HERE, '..', 'models'))
MODEL_PATH = os.path.join(MODELS_DIR, 'momentum_filter_v1.json')

with open(MODEL_PATH, 'r', encoding='utf-8') as f:
    MODEL_CFG = json.load(f)

FILTER_LOGIC = (MODEL_CFG.get('filter_logic') or 'all').lower()
FILTERS = MODEL_CFG.get('filters', [])
ENTRY = MODEL_CFG.get('entry_model', {}) or {}
ENTRY_PARAMS = ENTRY.get('params', {})

# --- Helpers -----------------------------------------------------------------
OPMAP = {
    '>=': lambda a, b: a >= b,
    '<=': lambda a, b: a <= b,
    '>':  lambda a, b: a >  b,
    '<':  lambda a, b: a <  b,
    '==': lambda a, b: a == b,
    '!=': lambda a, b: a != b,
}

def _get_field(snapshot: dict, field: str):
    # Snapshot fields as produced by DataHub
    return snapshot.get(field)

def _eval_filter(snap: dict, flt: dict) -> bool:
    field = flt.get('field')
    typ = (flt.get('type') or 'numeric').lower()
    op = (flt.get('operator') or '').replace('&gt;=', '>=').replace('&lt;=', '<=') \
                                     .replace('&gt;', '>').replace('&lt;', '<')
    val = flt.get('value')
    if op not in OPMAP:
        log.warning(f'Unknown operator "{op}" in filter; skipping -> False')
        return False
    x = _get_field(snap, field)
    if x is None:
        return False
    try:
        if typ == 'numeric':
            x = float(x); v = float(val)
        else:
            v = val
    except Exception:
        return False
    return OPMAPx, v

def filters_pass(snap: dict) -> bool:
    results = [ _eval_filter(snap, f) for f in FILTERS ]
    if not results:
        return True
    return all(results) if FILTER_LOGIC == 'all' else any(results)

# --- Per-symbol rolling state for entry logic --------------------------------
class SymState:
    __slots__ = ('prices', 'last_entry_ts')
    def __init__(self, lookback: int):
        self.prices = deque(maxlen=max(lookback, 1))
        self.last_entry_ts = 0.0

LOOKBACK = int(ENTRY_PARAMS.get('lookback', 5))
THRESH  = float(ENTRY_PARAMS.get('threshold', 1.2))
COOLDOWNS = int(ENTRY_PARAMS.get('cooldown_seconds', 60))
MIN_VOL = float(ENTRY_PARAMS.get('min_volatility', 0.5))
THROTTLE = float(ENTRY_PARAMS.get('throttle', 1.0))
TORQUE   = float(ENTRY_PARAMS.get('torque', 0.5))

SYMSTATE: dict[str, SymState] = defaultdict(lambda: SymState(LOOKBACK))

def maybe_emit_entry(sym: str, snap: dict, now_ts: float):
    # Gather values
    price = snap.get('last_price')
    momentum = float(snap.get('momentum') or 0.0)
    vol = float(snap.get('volatility') or 0.0)

    st = SYMSTATE[sym]
    if price is not None:
        st.prices.append(float(price))

    # Cooldown
    if now_ts - st.last_entry_ts < COOLDOWNS:
        return

    # Minimum volatility gate
    if vol < MIN_VOL:
        return

    # Momentum breakout condition:
    #  - require momentum >= threshold
    #  - require price >= recent_max to avoid repeated triggers in chop
    recent_max = max(st.prices) if st.prices else (price or 0.0)
    if momentum >= THRESH and price is not None and price >= recent_max:
        BUS.publish('signals', {
            'type': 'signal.entry',
            'ts': utcnow().isoformat(),
            'symbol': sym,
            'model': MODEL_CFG.get('model_name', 'model'),
            'model_version': MODEL_CFG.get('version', '1.0.0'),
            'params': {'throttle': THROTTLE, 'torque': TORQUE},
            'risk': {'stop_points': _exit_param('fixed_stop', 'stop_points', 5),
                     'target_points': _exit_param('fixed_gain', 'target_points', 10)},
            'source': 'evaluator'
        })
        st.last_entry_ts = now_ts

def _exit_param(model_type: str, key: str, default_val):
    for m in MODEL_CFG.get('exit_models', []):
        if (m.get('type') or '').lower() == model_type:
            return m.get('params', {}).get(key, default_val)
    return default_val

# --- Main loop ---------------------------------------------------------------
def _loop():
    log.info(f"Evaluator running with model: {MODEL_CFG.get('model_name')} v{MODEL_CFG.get('version')}")
    while True:
        now_ts = time.time()
        for sym, info in REGISTRY.symbols().items():
            snap = info.snapshot
            if not snap:
                continue
            if filters_pass(snap):
                maybe_emit_entry(sym, snap, now_ts)
        time.sleep(0.5)

def main():
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
