import telebot
import threading
import ccxt
import os
from dotenv import load_dotenv
import pandas as pd
import pytz
from datetime import datetime
import time
import ta.volatility
import json
import tempfile
import shutil
import logging
from logging.handlers import TimedRotatingFileHandler
import gzip
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Any, Dict

# --- LIBRERÍAS DE SOPORTE ---
try:
    import dateutil.parser as dateutil_parser
except Exception:
    dateutil_parser = None

try:
    from jsonschema import validate as jsonschema_validate, ValidationError
except Exception:
    jsonschema_validate = None
    ValidationError = Exception

# --- CONFIGURACIÓN INICIAL ---
load_dotenv()
TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
bot = telebot.TeleBot(TOKEN)

# VARIABLES DE ESTADO GLOBALES
trading_active = False
OPEN_POSITIONS: List[Dict[str, Any]] = []
CLOSED_TRADES: List[Dict[str, Any]] = []

# PARÁMETROS FIJADOS
TARGET_ASSETS = ['BTC/USD', 'ADA/USD', 'XRP/USD', 'SOL/USD', 'ETH/USD', 'LTC/USD', 'DOT/USD', 'BCH/USD', 'UNI/USD', 'LINK/USD']
OPTIMAL_ATR_MULTIPLIER = 0.05
HOURS_TO_ANALYZE = 50
KILL_ZONE_START = 14
KILL_ZONE_END = 18
POSITIONS_FILE = 'open_positions.json'

# --- 1. MODELO DE DATOS (DATACLASS) ---
@dataclass
class Position:
    symbol: str
    direction: str
    entry_price: float
    amount_base: float
    stop_loss: Optional[float]
    take_profit: Optional[float]
    status: str
    open_time: Any = field(default_factory=lambda: datetime.now(pytz.utc))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if isinstance(d.get('open_time'), datetime):
            d['open_time'] = d['open_time'].isoformat()
        return d

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> 'Position':
        ot = d.get('open_time')
        if isinstance(ot, str):
            if dateutil_parser:
                ot_parsed = dateutil_parser.parse(ot)
            else:
                ot_parsed = datetime.fromisoformat(ot)
            d['open_time'] = ot_parsed
        return Position(**d)

# --- 2. LOGGING CON ROTACIÓN Y COMPRESIÓN ---
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'kraken.log')

def _rotator(source, dest):
    try:
        with open(source, 'rb') as sf, gzip.open(dest + '.gz', 'wb') as df:
            shutil.copyfileobj(sf, df)
        os.remove(source)
    except Exception:
        try: shutil.move(source, dest)
        except Exception: pass

file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight', interval=1, backupCount=30, utc=True, encoding='utf-8')
file_handler.rotator = _rotator
file_handler.namer = lambda name: name + '.gz'
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', handlers=[file_handler, logging.StreamHandler()])

# --- 3. CONEXIÓN Y PERSISTENCIA (EL CORAZÓN DEL BOT) ---
def initialize_kraken_exchange():
    try:
        exchange = ccxt.kraken({
            'apiKey': os.getenv('KRAKEN_API_KEY'),
            'secret': os.getenv('KRAKEN_SECRET'),
            'enableRateLimit': True,
        })
        logging.info("Conexión a Kraken inicializada.")
        return exchange
    except Exception as e:
        logging.error(f"Error al inicializar Kraken: {e}")
        return None

_SAVE_LOCK = threading.Lock()
_LAST_SAVE_TIME = 0.0
_SAVE_DEBOUNCE_SECONDS = 1.0
_SAVE_PENDING = False

def load_open_positions():
    global OPEN_POSITIONS
    try:
        if os.path.exists(POSITIONS_FILE):
            with open(POSITIONS_FILE, 'r') as f:
                data = json.load(f)
                loaded = []
                for item in data:
                    try:
                        pos = Position.from_dict(item)
                        loaded.append(pos.to_dict())
                    except Exception:
                        loaded.append(item)
                OPEN_POSITIONS = loaded
                logging.info(f"{len(OPEN_POSITIONS)} posiciones cargadas.")
    except Exception as e:
        logging.warning(f"Error cargando posiciones: {e}")

def save_open_positions():
    def _write_atomic(data_to_write):
        dirpath = os.path.dirname(os.path.abspath(POSITIONS_FILE)) or '.'
        fd, tmp_path = tempfile.mkstemp(prefix='._op_', dir=dirpath)
        try:
            with os.fdopen(fd, 'w') as tmpf:
                json.dump(data_to_write, tmpf, indent=4, default=str)
                tmpf.flush()
                os.fsync(tmpf.fileno())
            shutil.move(tmp_path, POSITIONS_FILE)
        finally:
            if os.path.exists(tmp_path):
                try: os.remove(tmp_path)
                except Exception: pass

    with _SAVE_LOCK:
        global _LAST_SAVE_TIME, _SAVE_PENDING
        now = time.time()
        data_snapshot = [p.to_dict() if isinstance(p, Position) else p for p in OPEN_POSITIONS]

        if now - _LAST_SAVE_TIME < _SAVE_DEBOUNCE_SECONDS:
            if not _SAVE_PENDING:
                _SAVE_PENDING = True
                def _delayed():
                    global _SAVE_PENDING, _LAST_SAVE_TIME
                    _write_atomic(data_snapshot)
                    _LAST_SAVE_TIME = time.time()
                    _SAVE_PENDING = False
                threading.Timer(_SAVE_DEBOUNCE_SECONDS, _delayed).start()
            return
        _write_atomic(data_snapshot)
        _LAST_SAVE_TIME = now

# --- 4. LÓGICA DE ANÁLISIS (NUESTRAS FUNCIONES ORIGINALES) ---
def fetch_recent_data(exchange, symbol, timeframe='1h', limit=50):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv: return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        logging.error(f"Error fetch {symbol}: {e}")
        return None

def preprocess_data_for_time_bias(df):
    df = df.set_index(df['timestamp'])
    if df.index.tz is None:
        df.index = df.index.tz_localize(pytz.utc)
    df['hour_utc'] = df.index.hour
    df['candle_range'] = df['high'] - df['low']
    return df.reset_index(drop=True)

def mark_kill_zones(df):
    df['is_kill_zone'] = (df['hour_utc'] >= KILL_ZONE_START) & (df['hour_utc'] < KILL_ZONE_END)
    return df

def analyze_gross_return(df):
    df['gross_return'] = df['close'] - df['open']
    kill_zone_gr = df[df['is_kill_zone'] == True]['gross_return'].mean()
    low_liquidity_gr = df[df['is_kill_zone'] == False]['gross_return'].mean()
    
    if pd.isna(kill_zone_gr): return 0.0
    
    logging.info(f"KZ Mean: {kill_zone_gr:.2f} | Low Liq Mean: {low_liquidity_gr:.2f}")
    return kill_zone_gr

def calculate_atr(df, window=20): 
    df['atr'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], window=window)
    return df['atr'].iloc[-1]

def calculate_exit_levels(entry_price, atr_value, direction):
    risk_amount = atr_value * 1.5
    profit_amount = atr_value * 3.0
    if direction == "LONG (COMPRA)":
        return round(entry_price - risk_amount, 2), round(entry_price + profit_amount, 2)
    elif direction == "SHORT (VENTA)":
        return round(entry_price + risk_amount, 2), round(entry_price - profit_amount, 2)
    return None, None

# --- 5. EJECUCIÓN Y TRADING ENGINE ---
def execute_trade_simulation(symbol, bias_score, atr_multiplier_value, historical_data): 
    global OPEN_POSITIONS
    try:
        entry_price = historical_data['close'].iloc[-1] 
        atr_value = calculate_atr(historical_data.copy())
        open_time = historical_data.iloc[-1]['timestamp']
        dynamic_threshold = atr_value * atr_multiplier_value 
    except Exception as e:
        logging.error(f"Error cálculo parámetros {symbol}: {e}")
        return

    # Filtro Robusto ATR
    if not (0.05 < atr_value < 100.0):
        logging.info(f"Mantenese al margen {symbol}: ATR {atr_value:.2f} fuera de rango.")
        return

    direction = "LONG (COMPRA)" if bias_score > dynamic_threshold else "SHORT (VENTA)" if bias_score < -dynamic_threshold else "NEUTRAL"

    if direction != "NEUTRAL":
        sl, tp = calculate_exit_levels(entry_price, atr_value, direction)
        pos_obj = Position(symbol=symbol, direction=direction, entry_price=entry_price, 
                           amount_base=100/entry_price, stop_loss=sl, take_profit=tp, 
                           status='OPEN', open_time=open_time)
        OPEN_POSITIONS.append(pos_obj.to_dict())
        save_open_positions()
        bot.send_message(CHAT_ID, f"🟢 *ORDEN SIMULADA*\n{symbol} | {direction}\nEntrada: ${entry_price:.2f}\nSL: ${sl} | TP: ${tp}")

def execute_live_trade(exchange, symbol, atr_multiplier=0.05, timeframe='1h', hours_to_analyze=50):
    historical_data = fetch_recent_data(exchange, symbol, timeframe, limit=hours_to_analyze)
    if historical_data is None or historical_data.empty: return
    
    processed_data = preprocess_data_for_time_bias(historical_data)
    data_with_zones = mark_kill_zones(processed_data)
    time_bias_score = analyze_gross_return(data_with_zones)

    execute_trade_simulation(symbol, time_bias_score, atr_multiplier, historical_data)

def monitor_and_close_positions(current_price_data, exchange):
    global OPEN_POSITIONS, CLOSED_TRADES
    now_utc = datetime.now(pytz.utc)
    time_exit_allowed = (now_utc.hour >= KILL_ZONE_END)

    for i in range(len(OPEN_POSITIONS) - 1, -1, -1):
        pos = OPEN_POSITIONS[i]
        price = current_price_data.get(pos['symbol'])
        if price is None: continue

        exit_reason = None
        if pos['direction'] == 'LONG (COMPRA)':
            if price >= pos['take_profit']: exit_reason = "TAKE PROFIT (TP)"
            elif price <= pos['stop_loss']: exit_reason = "STOP LOSS (SL)"
        elif pos['direction'] == 'SHORT (VENTA)':
            if price <= pos['take_profit']: exit_reason = "TAKE PROFIT (TP)"
            elif price >= pos['stop_loss']: exit_reason = "STOP LOSS (SL)"

        if exit_reason is None and time_exit_allowed:
            exit_reason = "TIME EXIT (KZ EXPIRÓ)"

        if exit_reason:
            pnl_usd = (price - pos['entry_price']) * pos['amount_base']
            if pos['direction'] == 'SHORT (VENTA)': pnl_usd = -pnl_usd
            
            pos.update({'status': 'CLOSED', 'exit_price': price, 'exit_reason': exit_reason, 'pnl_usd': pnl_usd})
            CLOSED_TRADES.append(OPEN_POSITIONS.pop(i))
            save_open_positions()
            bot.send_message(CHAT_ID, f"🏁 *CIERRE {pos['symbol']}*\nMotivo: {exit_reason}\nPnL: ${pnl_usd:.2f}")

# --- 6. REPORTES Y COMANDOS ---
def print_final_trade_report():
    if not CLOSED_TRADES: return
    df = pd.DataFrame(CLOSED_TRADES)
    total_pnl = df['pnl_usd'].sum()
    win_rate = (len(df[df['pnl_usd'] > 0]) / len(df)) * 100
    report = f"📊 *AUDITORÍA FINAL*\nTrades: {len(df)}\nWin Rate: {win_rate:.2f}%\nPNL: ${total_pnl:.2f}"
    bot.send_message(CHAT_ID, report)

@bot.message_handler(commands=['start_trading'])
def handle_start(message):
    global trading_active
    if str(message.chat.id) == CHAT_ID:
        trading_active = True
        bot.reply_to(message, "🚀 *SISTEMA INICIADO*")
        threading.Thread(target=run_initial_positions, args=(kraken,)).start()

def run_initial_positions(exchange):
    for symbol in TARGET_ASSETS:
        execute_live_trade(exchange, symbol, OPTIMAL_ATR_MULTIPLIER, '1h', HOURS_TO_ANALYZE)

@bot.message_handler(commands=['stop_trading'])
def handle_stop(message):
    global trading_active
    trading_active = False
    bot.reply_to(message, "🛑 *DETENIDO*")
    print_final_trade_report()

def trading_loop(exchange):
    logging.info("Motor de vigilancia iniciado.")
    while True:
        if trading_active and OPEN_POSITIONS:
            try:
                real_current_prices = {s: exchange.fetch_ticker(s)['last'] for s in set(p['symbol'] for p in OPEN_POSITIONS)}
                monitor_and_close_positions(real_current_prices, exchange)
            except Exception as e:
                logging.error(f"Error en bucle: {e}")
        time.sleep(60)

# --- INICIO ---
if __name__ == "__main__":
    kraken = initialize_kraken_exchange()
    if kraken:
        load_open_positions()
        threading.Thread(target=lambda: bot.polling(none_stop=True), daemon=True).start()
        bot.send_message(CHAT_ID, "🖥️ *BOT ONLINE*")
        trading_loop(kraken)