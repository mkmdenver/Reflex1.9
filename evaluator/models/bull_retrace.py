# Minimal bull retrace: positive momentum and recent pullback score

def detect_bull_retrace(symbol: str, snapshot: dict, cfg: dict):
    m = snapshot.get('momentum', 0)
    vol = snapshot.get('volatility', 0)
    score = max(0.0, m*cfg.get('momentum_weight',1.0) - vol*cfg.get('vol_penalty',0.3))
    if score >= cfg.get('score_min', 0.2):
        return {'score': round(score,3)}
    return None
