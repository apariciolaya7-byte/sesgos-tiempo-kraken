from flask import Flask, jsonify, send_from_directory, abort
from flask_cors import CORS
import pandas as pd
import os

app = Flask(__name__, static_folder='frontend', static_url_path='')
CORS(app)

BASE = os.path.abspath(os.path.dirname(__file__))

@app.route('/')
def index():
    return send_from_directory(os.path.join(BASE, 'frontend'), 'index.html')

@app.route('/<path:path>')
def static_proxy(path):
    return send_from_directory(os.path.join(BASE, 'frontend'), path)

@app.route('/api/analysis_files')
def analysis_files():
    files = [f for f in os.listdir(BASE) if f.endswith('_time_bias_hourly_analysis.csv')]
    symbols = [f.replace('_time_bias_hourly_analysis.csv','') for f in files]
    return jsonify(symbols)

@app.route('/api/analysis/<symbol>')
def analysis(symbol):
    filename = f"{symbol}_time_bias_hourly_analysis.csv"
    path = os.path.join(BASE, filename)
    if not os.path.exists(path):
        return abort(404, description='Archivo no encontrado')
    df = pd.read_csv(path)
    # Convertir a tipos simples
    data = df.fillna(0).to_dict(orient='records')
    return jsonify(data)

@app.route('/api/positions')
def positions():
    path = os.path.join(BASE, 'open_positions.json')
    if not os.path.exists(path):
        return jsonify([])
    import json
    with open(path, 'r') as f:
        data = json.load(f)
    return jsonify(data)

@app.route('/api/backtest')
def backtest():
    path = os.path.join(BASE, 'backtesting_results.csv')
    if not os.path.exists(path):
        return jsonify([])
    df = pd.read_csv(path)
    return jsonify(df.fillna(0).to_dict(orient='records'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
