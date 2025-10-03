import csv, os
from ..models.base import Signal

class ModelLogger:
    def __init__(self, path='model_events.csv'):
        self.path = path
        if not os.path.exists(self.path):
            with open(self.path, 'w', newline='') as f:
                w = csv.writer(f); w.writerow(['ts','symbol','model','score','note'])
    def log(self, ts, symbol, model, score, note=''):
        with open(self.path, 'a', newline='') as f:
            w = csv.writer(f); w.writerow([ts,symbol,model,score,note])
