from collections import deque

class DoubleRingBuffer:
    def __init__(self, short=256, long=4096):
        self.trades = deque(maxlen=short)
        self.quotes = deque(maxlen=short)
        self.trades_long = deque(maxlen=long)
        self.quotes_long = deque(maxlen=long)

    def add_trade(self, t):
        self.trades.append(t); self.trades_long.append(t)
    def add_quote(self, q):
        self.quotes.append(q); self.quotes_long.append(q)
    def stats(self):
        return { 'trades_short': len(self.trades), 'quotes_short': len(self.quotes) }
