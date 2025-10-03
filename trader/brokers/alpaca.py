import json, ssl, urllib.request
from common.config import Config
from common.logger import setup_logger

log = setup_logger('trader.alpaca')

class Alpaca:
    def __init__(self):
        self.base = Config.ALPACA_BASE_URL
        self.key = Config.ALPACA_API_KEY
        self.secret = Config.ALPACA_API_SECRET

    @property
    def is_configured(self):
        return bool(self.base and self.key and self.secret)

    def _req(self, path: str, payload: dict):
        url = f"{self.base}{path}"
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        req.add_header('APCA-API-KEY-ID', self.key)
        req.add_header('APCA-API-SECRET-KEY', self.secret)
        req.add_header('Content-Type', 'application/json')
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx) as resp:
            return json.loads(resp.read().decode('utf-8'))

    def market_buy(self, symbol: str, qty: int):
        if not self.is_configured:
            return False, 0.0, 'no_credentials'
        try:
            r = self._req('/v2/orders', {
                'symbol': symbol, 'qty': qty, 'side': 'buy',
                'type': 'market', 'time_in_force': 'day'
            })
            filled = float(r.get('filled_avg_price') or 0.0)
            return True, filled, f"order_id={r.get('id')}"
        except Exception as e:
            log.error(f"Alpaca buy error: {e}")
            return False, 0.0, f"error={e}"

    def market_sell(self, symbol: str, qty: int):
        if not self.is_configured:
            return False, 0.0, 'no_credentials'
        try:
            r = self._req('/v2/orders', {
                'symbol': symbol, 'qty': qty, 'side': 'sell',
                'type': 'market', 'time_in_force': 'day'
            })
            filled = float(r.get('filled_avg_price') or 0.0)
            return True, filled, f"order_id={r.get('id')}"
        except Exception as e:
            log.error(f"Alpaca sell error: {e}")
            return False, 0.0, f"error={e}"
