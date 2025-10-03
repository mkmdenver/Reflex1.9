import random, threading, time
from datahub.registry import REGISTRY
from datahub.snapshots import update_snapshot
from datahub.events import publish_event

class SimFeed:
    def __init__(self, symbols=None, interval=0.5):
        self.symbols = symbols or list(REGISTRY.symbols().keys())
        self.interval = interval
        self._stop = False

    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def stop(self):
        self._stop = True

    def _run(self):
        prices = {s: 100.0 + random.random()*10 for s in self.symbols}
        while not self._stop:
            for s in self.symbols:
                delta = random.uniform(-0.2, 0.2)
                prices[s] = max(1.0, prices[s] + delta)
                momentum = delta * 5
                vol = random.uniform(1e5, 5e6)
                update_snapshot(s, prices[s], vol, momentum, 'stage1_pass')
            publish_event('tick.batch', payload={'n': len(self.symbols)})
            time.sleep(self.interval)
