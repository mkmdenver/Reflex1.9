from flask import Flask, render_template, jsonify
from common.config import Config
from common.logger import setup_logger
import threading, argparse, requests

log = setup_logger('cockpit')
app = Flask(__name__, template_folder='templates', static_folder='static')

DATAHUB = f"http://{Config.DATAHUB_HOST}:{Config.DATAHUB_PORT}"

def _ensure_sim_running():
    try:
        requests.post(f"{DATAHUB}/control/sim", json={'mode':'start'}, timeout=2)
    except Exception:
        pass

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/api/symbols')
def api_symbols():
    try:
        r = requests.get(f"{DATAHUB}/symbols", timeout=2)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=Config.COCKPIT_PORT)
    args = parser.parse_args()
    # kick the sim feed
    threading.Thread(target=_ensure_sim_running, daemon=True).start()
    from waitress import serve
    log.info(f"Starting Cockpit on 127.0.0.1:{args.port}")
    serve(app, host='127.0.0.1', port=args.port)
