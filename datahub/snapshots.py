from datahub.registry import REGISTRY
from datetime import datetime, timezone


def update_snapshot(symbol: str, price: float, volume: float, momentum: float, filter_status: str):
    si = REGISTRY.upsert(symbol)
    si.last_price = price
    si.snapshot.update({
        'last_price': price,                     # <— include explicit last price
        'volatility': round(abs(momentum) * 0.8, 3),
        'volume': volume,
        'momentum': momentum,
        'filter_status': filter_status,
        'timestamp': datetime.now(timezone.utc).isoformat()
    })

