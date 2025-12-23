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
import logging
from logging.handlers import TimedRotatingFileHandler
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Any, Dict


BANK_FILE = 'virtual_bank.json'

def get_virtual_balance():
    try:
        with open(BANK_FILE, 'r') as f:
            data = json.load(f)
            return data.get('balance', 500.0)
    except FileNotFoundError:
        return 500.0

def update_virtual_balance(amount):
    current = get_virtual_balance()
    new_balance = current + amount
    with open(BANK_FILE, 'w') as f:
        json.dump({"balance": round(new_balance, 2)}, f)
    return new_balance

# --- 1. CONFIGURACIÓN GLOBAL (Accesible para todas las funciones) ---
load_dotenv()
TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
bot = telebot.TeleBot(TOKEN)

# Variables de Control
trading_active = False 
TARGET_ASSETS = ['BTC/USD', 'ADA/USD', 'XRP/USD', 'SOL/USD', 'ETH/USD', 'LTC/USD', 'DOT/USD', 'BCH/USD', 'UNI/USD', 'LINK/USD']
OPTIMAL_ATR_MULTIPLIER = 0.05
HOURS_TO_ANALYZE = 50

OPEN_POSITIONS: List[Dict[str, Any]] = []
CLOSED_TRADES: List[Dict[str, Any]] = []
POSITIONS_FILE = 'open_positions.json'


# --- 1.5 CEREBRO DE INTELIGENCIA Y AUDITORÍA ---

class TradingAuditor:
    def __init__(self, max_simultaneous=3, daily_loss_limit=25.0):
        self.max_simultaneous = max_simultaneous
        self.daily_loss_limit = daily_loss_limit

    def check_safety(self, symbol, current_positions, current_balance):
        # Evitamos la "metralleta" de 273 órdenes
        if len(current_positions) >= self.max_simultaneous:
            return False, f"Límite de {self.max_simultaneous} posiciones alcanzado."
        
        if any(p['symbol'] == symbol for p in current_positions):
            return False, f"Ya operando {symbol}."
            
        # Stop Loss Global: Protegemos los $500
        if (500.0 - current_balance) >= self.daily_loss_limit:
            return False, "DRAWDOWN CRÍTICO: Operativa bloqueada por hoy."

        return True, "OK"

def estratega_no_supervisado(df):
    """ Busca patrones de 'ruido' vs 'tendencia' """
    kz_data = df[df['is_kill_zone'] == True]
    if len(kz_data) < 2: return "NEUTRAL"

    # Calculamos la 'limpieza' del movimiento
    cuerpo_promedio = (kz_data['close'] - kz_data['open']).abs().mean()
    rango_promedio = kz_data['candle_range'].mean()
    coherencia = cuerpo_promedio / rango_promedio if rango_promedio > 0 else 0

    if coherencia > 0.6: return "TENDENCIA_SOLIDA"
    if coherencia < 0.3: return "RUIDO_LATERAL"
    return "NEUTRAL"

# Instanciamos al Auditor
auditor = TradingAuditor(max_simultaneous=3, daily_loss_limit=25.0)

# --- 2. MODELO DE DATOS (Tu estructura original) ---
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

# --- 3. PERSISTENCIA Y LOGS ---
def load_open_positions():
    global OPEN_POSITIONS
    if os.path.exists(POSITIONS_FILE):
        try:
            with open(POSITIONS_FILE, 'r') as f:
                OPEN_POSITIONS = json.load(f)
        except: OPEN_POSITIONS = []

def save_open_positions():
    with open(POSITIONS_FILE, 'w') as f:
        json.dump(OPEN_POSITIONS, f, indent=4, default=str)

# --- 4. LÓGICA DE TRADING (Respetando tu Bloc de Notas) ---

def calculate_exit_levels(symbol, entry_price, atr_value, direction):
    # Mejora de precisión para evitar cierres erróneos en UNI/ADA
    precision = 4 if entry_price < 10 else 2
    risk_amount = atr_value * 1.5
    profit_amount = atr_value * 3.0

    if direction == "LONG (COMPRA)":
        sl, tp = entry_price - risk_amount, entry_price + profit_amount
    else:
        sl, tp = entry_price + risk_amount, entry_price - profit_amount
    return round(sl, precision), round(tp, precision)

def execute_trade_simulation(symbol, bias_score, atr_multiplier_value, historical_data): 
    global OPEN_POSITIONS
    # FILTRO ANTI-DUPLICADOS
    if any(p['symbol'] == symbol for p in OPEN_POSITIONS): return

    entry_price = historical_data['close'].iloc[-1] 
    atr_value = ta.volatility.average_true_range(historical_data['high'], historical_data['low'], historical_data['close'], window=20).iloc[-1]
    
    threshold = atr_value * atr_multiplier_value 
    direction = "LONG (COMPRA)" if bias_score > threshold else "SHORT (VENTA)" if bias_score < -threshold else None

    if direction:
        sl, tp = calculate_exit_levels(symbol, entry_price, atr_value, direction)
        pos = Position(symbol=symbol, direction=direction, entry_price=entry_price, 
                       amount_base=100/entry_price, stop_loss=sl, take_profit=tp, status='OPEN')
        OPEN_POSITIONS.append(pos.to_dict())
        save_open_positions()
        bot.send_message(CHAT_ID, f"🟢 *NUEVA ORDEN*\n{symbol} | {direction}\nEntrada: ${entry_price:.2f}\nSL: ${sl}")

# --- 5. COMANDOS TELEGRAM (Control de Usuario) ---

# --- 2. LÓGICA DE CONTROL TEMPORAL (Basada en tu Backtesting) ---
def is_in_kill_zone():
    now_utc = datetime.now(pytz.utc).hour
    return KILL_ZONE_START <= now_utc < KILL_ZONE_END

# --- 3. EL MOTOR DE EJECUCIÓN (Corregido) ---
def run_trading_cycle(exchange):
    """
    Esta es la única función que el bucle principal llamará.
    """
    global trading_active
    
    # A. Verificación de Ventana (Evita operar a las 21:00)
    if not is_in_kill_zone():
        logging.info(f"⏳ Fuera de Kill Zone ({KILL_ZONE_START}-{KILL_ZONE_END} UTC). Monitoreando cierres únicamente.")
    else:
        # Solo abrimos posiciones si estamos EN la hora y el bot está START
        logging.info("[MODULO 1] Analizando aperturas...")
        for symbol in TARGET_ASSETS:
            execute_live_trade(exchange, symbol, OPTIMAL_ATR_MULTIPLIER, '1h', HOURS_TO_ANALYZE)

    # B. Monitoreo de Cierres (Siempre activo si hay posiciones)
    if OPEN_POSITIONS:
        logging.info("[MODULO 2] Monitoreando SL/TP/Time Exit...")
        real_current_prices = {}
        for symbol in TARGET_ASSETS:
            try:
                ticker = exchange.fetch_ticker(symbol)
                real_current_prices[symbol] = ticker['last']
            except: continue
        
        monitor_and_close_positions(real_current_prices, exchange)
    
    # C. Reporte si se cerró algo
    if CLOSED_TRADES:
        print_final_trade_report()

@bot.message_handler(commands=['start_trading'])
def handle_start(message):
    global trading_active
    if str(message.chat.id) == CHAT_ID:
        trading_active = True
        
        # --- NUEVA LÓGICA DE FEEDBACK INSTANTÁNEO ---
        now_utc = datetime.now(pytz.utc)
        current_hour = now_utc.hour
        
        status_msg = "🚀 *SISTEMA ACTIVADO*\n\n"
        status_msg += f"⏰ Hora actual: {now_utc.strftime('%H:%M')} UTC\n"
        status_msg += f"📅 Ventana: {KILL_ZONE_START}:00 - {KILL_ZONE_END}:00 UTC\n\n"
        
        if KILL_ZONE_START <= current_hour < KILL_ZONE_END:
            status_msg += "✅ *ESTADO:* En ventana operativa. ¡Buscando entradas ahora mismo!"
        else:
            # Calculamos cuánto falta para las 14:00 (opcional, pero muy pro)
            wait_hours = (KILL_ZONE_START - current_hour) % 24
            status_msg += f"⏳ *ESTADO:* Fuera de horario. El bot entrará en modo análisis en {wait_hours} horas."

        bot.reply_to(message, status_msg, parse_mode='Markdown')
        
        # Iniciar hilo si no está corriendo
        if not any(t.name == "TradingThread" for t in threading.enumerate()):
            threading.Thread(target=run_initial_cycle, name="TradingThread").start()
    else:
        bot.reply_to(message, "❌ No autorizado.")


def run_trading_cycle(kraken):
    global trading_active
    if not trading_active:
        return

    # Diccionario para recolectar qué pasó con cada moneda en esta vuelta
    reporte_vuelta = {}
    
    # Lista de tus monedas: ADA, LINK, BCH, ETH, BTC, UNI, SOL, DOT
    for symbol in TARGET_ASSETS:
        try:
            # Capturamos el diccionario que devuelve execute_live_trade
            resultado = execute_live_trade(kraken, symbol)
            reporte_vuelta[symbol] = resultado
        except Exception as e:
            reporte_vuelta[symbol] = {"veredicto": f"ERROR: {str(e)[:10]}", "bias": 0}

    # Una vez analizadas todas, enviamos el informe único
    enviar_informe_telegram(reporte_vuelta)


def enviar_informe_telegram(data_reporte):
    ahora_utc = datetime.now(pytz.utc).strftime('%H:%M')
    balance = get_virtual_balance() # Para saber cómo va la cuenta
    
    msg = f"🛰️ **INFORME DE RADAR | {ahora_utc} UTC**\n"
    msg += f"💰 **Balance Virtual:** ${balance:.2f}\n"
    msg += "----------------------------------\n"

    for symbol, info in data_reporte.items():
        veredicto = info.get('veredicto', 'N/A')
        bias = info.get('bias', 0)
        
        # Asignamos emoji según el veredicto
        if "EJECUTADO" in veredicto:
            status = "✅ ENTRADA"
        elif "AUDITOR" in veredicto:
            status = f"🛡️ BLOQUEO ({veredicto.split(':')[-1].strip()})"
        elif "RUIDO" in veredicto:
            status = "📉 RUIDO (Estratega)"
        elif "NEUTRAL" in veredicto:
            status = "⚪ NEUTRAL"
        else:
            status = f"❓ {veredicto}"

        msg += f"**{symbol}**: {status} | Bias: `{bias:.2f}`\n"

    msg += "----------------------------------\n"
    msg += "🧐 *Estado: Vigilando mercado...*"
    
    try:
        bot.send_message(CHAT_ID, msg, parse_mode='Markdown')
    except Exception as e:
        print(f"Error Telegram: {e}")


@bot.message_handler(commands=['stop_trading'])
def handle_stop(message):
    global trading_active
    trading_active = False
    bot.reply_to(message, "🛑 *SISTEMA DETENIDO*")



@bot.message_handler(commands=['report'])
def handle_report_request(message):
    """Permite consultar el estado de la jornada en cualquier momento."""
    if str(message.chat.id) == CHAT_ID:
        # Si hay posiciones abiertas, el reporte es parcial
        status_prefix = "🕒 *REPORTE PARCIAL (Jornada en curso)*\n"
        if not is_in_kill_zone():
            status_prefix = "🏁 *REPORTE DE JORNADA FINALIZADA*\n"
            
        bot.send_message(CHAT_ID, "📊 Generando auditoría solicitada...")
        print_final_trade_report(custom_prefix=status_prefix)
    else:
        bot.reply_to(message, "❌ No autorizado.")



@bot.message_handler(commands=['balance'])
def handle_balance(message):
    balance_actual = get_virtual_balance() # Lee el JSON de 500$
    num_pos = len(OPEN_POSITIONS)
    
    # Simulación de margen: restamos 10$ virtuales por cada posición abierta
    disponible = balance_actual - (num_pos * 10)
    
    msg = f"💰 *CONTABILIDAD VIRTUAL*\n"
    msg += f"----------------------------------\n"
    msg += f"💵 *Capital Total:* `${balance_actual:.2f}`\n"
    msg += f"📉 *Disponible:* `${max(0, disponible):.2f}`\n"
    msg += f"📦 *Posiciones:* {num_pos} activas\n"
    msg += f"----------------------------------\n"
    
    pnl_acumulado = balance_actual - 500.0
    status_icon = "📈" if pnl_acumulado >= 0 else "📉"
    msg += f"{status_icon} *PnL Histórico:* `${pnl_acumulado:.2f}`"
    
    bot.reply_to(message, msg, parse_mode='Markdown')


@bot.message_handler(commands=['status'])
def handle_status(message):
    """Auditoría rápida del estado del motor y la cuenta."""
    if str(message.chat.id) != CHAT_ID: return
    
    try:
        # 1. Datos de Cuenta
        balance = kraken.fetch_balance()
        usd_total = balance.get('USD', {}).get('total', 0)
        
        # 2. Datos del Bot
        status_bot = "🟢 ACTIVO" if trading_active else "🔴 DETENIDO"
        num_pos = len(OPEN_POSITIONS)
        window = "✅ DENTRO" if is_in_kill_zone() else "⏳ FUERA"
        
        msg = (
            f"🛡️ *AUDITORÍA DE SISTEMA*\n"
            f"----------------------------------\n"
            f"🤖 *Bot:* {status_bot}\n"
            f"🕒 *Kill Zone:* {window} (14-18 UTC)\n"
            f"📦 *Posiciones:* {num_pos} abiertas\n"
            f"💰 *Equity:* `${usd_total:.2f}`\n"
            f"----------------------------------\n"
            f"📡 *Conexión:* Kraken API OK"
        )
        bot.reply_to(message, msg, parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"⚠️ Error en auditoría: {e}")

def run_initial_cycle():
    """Ejecuta execute_live_trade para cada activo del TARGET_ASSETS global"""
    if not trading_active: return
    for symbol in TARGET_ASSETS:
        try:
            # Aquí llamamos a tu función original del bloc de notas
            execute_live_trade(kraken, symbol, OPTIMAL_ATR_MULTIPLIER, '1h', HOURS_TO_ANALYZE)
        except Exception as e:
            logging.error(f"Error en {symbol}: {e}")


def enviar_reporte_consolidado(diagnostico_total):
    """
    Recibe un diccionario con los resultados de todos los activos 
    y envía UN SOLO mensaje de Telegram.
    """
    ahora_utc = datetime.now(pytz.utc).strftime('%H:%M')
    informe = f"📊 **REPORTE DE CICLO - {ahora_utc} UTC**\n"
    informe += "----------------------------------\n"
    
    resumen_estados = {"ENTRADA": 0, "BLOQUEO": 0, "RUIDO": 0}

    for symbol, info in diagnostico_total.items():
        # Emoji y estado según el veredicto
        if info['veredicto'] == "EJECUTADO":
            emoji = "✅"
            resumen_estados["ENTRADA"] += 1
            detalle = "Orden enviada"
        elif "AUDITOR" in info['veredicto']:
            emoji = "🛡️"
            resumen_estados["BLOQUEO"] += 1
            detalle = info['veredicto'].split(":")[1] # El motivo del auditor
        elif info['veredicto'] == "RUIDO":
            emoji = "📉"
            resumen_estados["RUIDO"] += 1
            detalle = "Ruido/Mechas"
        else:
            emoji = "⚪"
            detalle = "Sin Sesgo/Neutral"

        informe += f"{emoji} **{symbol}**: {detalle}\n"

    informe += "----------------------------------\n"
    informe += f"📈 Resumen: {resumen_estados['ENTRADA']} ON | {resumen_estados['BLOQUEO']} BLOCK | {resumen_estados['RUIDO']} RUIDO"
    
    try:
        bot.send_message(CHAT_ID, informe, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Error enviando informe: {e}")

# --- 6. BUCLE PRINCIPAL ---

def trading_loop(exchange):
    while True:
        if trading_active:
            try:
                # Monitoreo de precios para cerrar posiciones
                prices = {}
                for symbol in TARGET_ASSETS:
                    ticker = exchange.fetch_ticker(symbol)
                    prices[symbol] = ticker['last']
                
                # Tu función monitor_and_close original
                monitor_and_close_positions(prices, exchange)
            except Exception as e:
                logging.error(f"Error en loop: {e}")
        time.sleep(60)

# NUEVA FUNCIÓN (o adaptación)
def fetch_recent_data(exchange, symbol='BTC/USD', timeframe='1h', limit=50):
    """
    Descarga el número limitado (N) de velas históricas más recientes.
    Recomendado para análisis en tiempo real.
    """
    try:
        # ccxt por defecto usa el parámetro 'limit' para obtener las velas más recientes.
        ohlcv = exchange.fetch_ohlcv(
            symbol, 
            timeframe, 
            limit=limit 
        )
        
        if not ohlcv:
            logging.warning(f"No se obtuvieron datos recientes para {symbol}.")
            return None
            
        # 4. Compilación de Datos en un único DataFrame
        headers = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df = pd.DataFrame(ohlcv, columns=headers)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        return df
        
    except Exception as e:
        logging.error(f"Error al obtener datos recientes para {symbol}: {e}")
        return None
    

def execute_live_trade(kraken, symbol, atr_multiplier=0.05, timeframe='1h', hours_to_analyze=50):
    global OPEN_POSITIONS, trading_active
    
    if not trading_active: 
        return {"veredicto": "STOPPED"}

    historical_data = fetch_recent_data(kraken, symbol, timeframe, limit=hours_to_analyze)
    if historical_data is None or historical_data.empty: 
        return {"veredicto": "ERROR_DATA"}

    processed_data = preprocess_data_for_time_bias(historical_data)
    data_with_zones = mark_kill_zones(processed_data)
    
    # --- PASO 1: EL ESTRATEGA ---
    estado_mercado = estratega_no_supervisado(data_with_zones)
    bias_score = analyze_gross_return(data_with_zones)
    
    # --- PASO 2: EL AUDITOR ---
    balance_actual = get_virtual_balance()
    is_safe, reason = auditor.check_safety(symbol, OPEN_POSITIONS, balance_actual)

    # --- LÓGICA DE RETORNO PARA EL INFORME ---
    
    if not is_safe:
        logging.info(f"🛡️ AUDITOR: {reason}")
        return {"veredicto": f"AUDITOR: {reason}", "bias": bias_score}

    if estado_mercado == "RUIDO_LATERAL":
        logging.info(f"📉 ESTRATEGA: Mercado errático en {symbol}.")
        return {"veredicto": "RUIDO", "bias": bias_score}

    # Si pasa los filtros, evaluamos si el Bias es suficiente para entrar
    # Aquí asumo que tienes un umbral, ej: abs(bias_score) > 0
    if abs(bias_score) < 0.1: # Ajusta este umbral según tus pruebas
        return {"veredicto": "NEUTRAL", "bias": bias_score}

    # EJECUCIÓN
    try:
        execute_trade_simulation(symbol, bias_score, atr_multiplier, historical_data)
        return {"veredicto": "EJECUTADO", "bias": bias_score}
    except Exception as e:
        return {"veredicto": f"ERROR_EXEC: {str(e)[:10]}", "bias": bias_score}  
      

def preprocess_data_for_time_bias(df):
    """
    Normaliza el timestamp a UTC y calcula la volatilidad de la vela.
    """
    
    # 1. Asegurar UTC (si el timestamp no tiene una zona horaria asignada)
    # Convertimos el timestamp a un índice de pandas para facilitar el manejo.
    df = df.set_index(df['timestamp'])
    
    # Localizar (o asignar) la zona horaria UTC. 
    # Usamos .tz_localize para ASIGNAR la zona horaria a datos 'naive' (sin zona horaria).
    if df.index.tz is None:
        df.index = df.index.tz_localize(pytz.utc)

    # 2. Crear Columna de Hora (Para la estrategia de Kill Zones)
    df['hour_utc'] = df.index.hour
    
    # 3. Calcular Rango (Volatilidad)
    df['candle_range'] = df['high'] - df['low']
    
    logging.info(f"Datos pre-procesados. Zona horaria: {df.index.tz}")
    return df.reset_index(drop=True)


# Tiempos de ejemplo para la superposición Londres/Nueva York:
# La Kill Zone es de 08:00 a 12:00 UTC (4 horas de alta volatilidad)
KILL_ZONE_START = 00
KILL_ZONE_END = 03

def mark_kill_zones(df):
    """
    Marca las velas que caen dentro de la Kill Zone de alta liquidez.
    """
    # 1. Crear una columna booleana que es True si la hora está dentro del rango
    df['is_kill_zone'] = (df['hour_utc'] >= KILL_ZONE_START) & (df['hour_utc'] < KILL_ZONE_END)
    
    logging.info("Kill Zones marcadas en el DataFrame.")
    return df


# NUEVO INDICADOR: Devolver el Retorno Bruto (GR) de la Kill Zone
def analyze_gross_return(df):
    """Calcula el Retorno Bruto Promedio (GR) por vela en la Kill Zone."""
    
    # Calcular el cambio absoluto por vela
    df['gross_return'] = df['close'] - df['open']
    
    # -----------------------------------------------------------------
    # CORRECCIÓN CRÍTICA: Filtrar por el valor booleano TRUE/FALSE
    # -----------------------------------------------------------------
    # 1. Calcular el retorno promedio en la Kill Zone (donde 'is_kill_zone' es True)
    kill_zone_gr = df[df['is_kill_zone'] == True]['gross_return'].mean()
    
    # 2. Calcular el retorno promedio fuera de la Kill Zone (donde 'is_kill_zone' es False)
    low_liquidity_gr = df[df['is_kill_zone'] == False]['gross_return'].mean()
    
    # Mostrar resultados en consola
    logging.info("Análisis de Retorno Bruto Promedio (por Vela):")
    logging.info("-" * 50)
    
    # Manejo de NaN para evitar errores
    if pd.isna(kill_zone_gr):
        logging.warning("KILL ZONE (14:00 a 18:00 UTC): NaN (Movimiento promedio)")
        sesgo = "Neutro (Error de Cálculo o Datos insuficientes)."
        return 0.0 # Devolver 0.0 en caso de error para que el if/elif del main no falle
        
    # Continuación si no es NaN
    logging.info(f"KILL ZONE (14:00 a 18:00 UTC): ${kill_zone_gr:.2f} (Movimiento promedio)")
    logging.info(f"LOW LIQUIDITY (Otras Horas): ${low_liquidity_gr:.2f} (Movimiento promedio)")
    logging.info("-" * 50)
    
    if kill_zone_gr > 0:
        sesgo = "Ligeramente Alcista (el precio tiende a subir)."
    elif kill_zone_gr < 0:
        sesgo = "Ligeramente Bajista (el precio tiende a bajar)."
    else:
        sesgo = "Neutro."
        
    logging.info(f"Sesgo de Dirección en la KILL ZONE: {sesgo}")
    
    # DEVUELVE el indicador clave: Retorno Bruto de la Kill Zone
    return kill_zone_gr



def trading_loop(exchange):
    """Vigilancia constante de SL/TP y Time Exit"""
    global trading_active
    logging.info("Motor de vigilancia iniciado.")
    
    while True:
        if trading_active:
            try:
                # MODULO 2: Monitoreo Real
                real_current_prices = {}
                for symbol in TARGET_ASSETS:
                    ticker = exchange.fetch_ticker(symbol)
                    real_current_prices[symbol] = ticker['last']
                
                # Ejecutar cierre por SL/TP o Tiempo
                monitor_and_close_positions(real_current_prices, exchange)
                
            except Exception as e:
                logging.error(f"Error en el bucle de vigilancia: {e}")
        
        # Dormir 60 segundos para no saturar la API (Rate Limit)
        time.sleep(60)



def calculate_atr(df, window=20): 
    """
    Calcula el Average True Range (ATR) para la volatilidad, utilizando una ventana
    de N velas (por defecto 20) para el cálculo del valor final.
    """
    # Usamos la ventana definida (ahora 20) para el cálculo del ATR.
    # Esto asegura que el valor ATR de la última vela refleje la volatilidad de las 20 velas anteriores.
    
    # Asegúrate de que las columnas 'high', 'low', 'close' estén presentes
    df['atr'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], window=window)
    
    # Devolver el ATR de la última vela (este valor ya es el resultado del cálculo de 20 periodos)
    return df['atr'].iloc[-1]

def calculate_exit_levels(entry_price, atr_value, direction):
    """Calcula los niveles de Stop Loss y Take Profit."""
    
    # Parámetros de Riesgo/Recompensa
    SL_MULTIPLIER = 1.5  # Asumir 1.5x el ATR de riesgo
    TP_MULTIPLIER = 3.0  # Asumir 3.0x el ATR de recompensa (R:R 1:2)
    
    risk_amount = atr_value * SL_MULTIPLIER
    profit_amount = atr_value * TP_MULTIPLIER

    if direction == "LONG (COMPRA)":
        # SL: Por debajo del precio de entrada
        stop_loss = entry_price - risk_amount
        # TP: Por encima del precio de entrada
        take_profit = entry_price + profit_amount
    
    elif direction == "SHORT (VENTA)":
        # SL: Por encima del precio de entrada
        stop_loss = entry_price + risk_amount
        # TP: Por debajo del precio de entrada
        take_profit = entry_price - profit_amount
    
    else:
        # En caso neutral, no hay niveles
        return None, None
    
    return round(stop_loss, 2), round(take_profit, 2)


def monitor_and_close_positions(current_price_data, exchange):
    """
    Monitorea posiciones abiertas y actualiza el saldo virtual al cerrar.
    """
    global OPEN_POSITIONS, CLOSED_TRADES 

    now_utc = datetime.now(pytz.utc)
    current_utc_hour = now_utc.hour
    
    # El Time Exit solo aplica DESPUÉS de la hora de cierre de la Kill Zone
    time_exit_allowed = (current_utc_hour >= KILL_ZONE_END)
    
    logging.info(f"--- [ MONITOREO ACTIVO ] --- Hora: {now_utc.strftime('%H:%M:%S')} UTC")

    # Recorrer de atrás hacia adelante para evitar errores de índice al eliminar
    for i in range(len(OPEN_POSITIONS) - 1, -1, -1):
        pos = OPEN_POSITIONS[i]
        
        if isinstance(pos, Position):
            pos = OPEN_POSITIONS[i] = pos.to_dict()
            
        symbol = pos['symbol']
        current_price = current_price_data.get(symbol)
        
        if current_price is None:
            continue
            
        exit_reason = None
        close_price = None 

        # 1. Lógica de SL/TP
        if pos['direction'] == 'LONG (COMPRA)':
            if current_price >= pos['take_profit']:
                exit_reason, close_price = "TAKE PROFIT (TP)", pos['take_profit']
            elif current_price <= pos['stop_loss']:
                exit_reason, close_price = "STOP LOSS (SL)", pos['stop_loss']
        
        elif pos['direction'] == 'SHORT (VENTA)':
            if current_price <= pos['take_profit']: 
                exit_reason, close_price = "TAKE PROFIT (TP)", pos['take_profit']
            elif current_price >= pos['stop_loss']: 
                exit_reason, close_price = "STOP LOSS (SL)", pos['stop_loss']

        # 2. Lógica de Time Exit
        if exit_reason is None and time_exit_allowed:
            exit_reason = "TIME EXIT (KZ EXPIRÓ)"
            close_price = current_price 
            
        # 3. EJECUCIÓN DEL CIERRE Y ACTUALIZACIÓN DE CAPITAL
        if exit_reason:
            # Calcular PnL
            pnl_usd = (close_price - pos['entry_price']) * pos['amount_base']
            if pos['direction'] == 'SHORT (VENTA)':
                pnl_usd = -pnl_usd 

            # --- PUNTO CRÍTICO: Actualización del Banco Virtual ---
            # Sumamos (o restamos) el resultado del trade al balance de 500$
            nuevo_saldo = update_virtual_balance(pnl_usd)
            # ------------------------------------------------------

            pnl_status = "GANANCIA ✅" if pnl_usd > 0 else "PÉRDIDA ❌"
            logging.info(f"💰 CIERRE {symbol} | {exit_reason} | PnL: ${pnl_usd:.2f} | Nuevo Saldo: ${nuevo_saldo:.2f}")
            
            # Registrar el cierre
            pos['status'] = 'CLOSED'
            pos['exit_price'] = close_price 
            pos['exit_reason'] = exit_reason
            pos['pnl_usd'] = pnl_usd 
            
            CLOSED_TRADES.append(OPEN_POSITIONS.pop(i))
            save_open_positions()

    if not OPEN_POSITIONS:
        logging.info("📭 Sin posiciones abiertas.")


# ----------------------------------------------------
# NUEVA FUNCIÓN: SIMULACIÓN DE ENTRADA DE TRADING
# ----------------------------------------------------

def execute_trade_simulation(symbol, bias_score, atr_multiplier_value, historical_data): 
    """
    Simula una orden de mercado con cálculo de Stop Loss y Take Profit.
    Ahora incluye filtro de robustez ATR Mín/Máx.
    """
    # --- CAMBIO CRÍTICO: Riesgo Dinámico ---
    saldo_actual = get_virtual_balance()
    # Arriesgamos el 1% del capital total por operación (50$ si hay 500$)
    amount_usd = saldo_actual * 0.01 
    
    entry_price = historical_data['close'].iloc[-1]
    amount_base = amount_usd / entry_price

    # 1. Obtener precios y calcular ATR
    try:
        entry_price = historical_data['close'].iloc[-1] 
        # Se llama a la función ATR, que ahora debe tener la lógica de las últimas 20 velas
        atr_value = calculate_atr(historical_data.copy())
        open_time = historical_data.index[-1]
        
        # CÁLCULO DEL UMBRAL DINÁMICO
        dynamic_threshold = atr_value * atr_multiplier_value 

    except Exception as e:
        logging.error(f"ERROR al calcular ATR/Precios para {symbol}: {e}")
        return
    
    # ----------------------------------------------------
    # NUEVO FILTRO DE ROBUSTEZ: ATR Mínimo y Máximo
    # ----------------------------------------------------
    # Se establecen límites de sentido común para evitar trades en volatilidad nula o extrema.
    MIN_ATR_USD = 0.05  
    MAX_ATR_USD = 100.0 

    if atr_value < MIN_ATR_USD:
        logging.info(f"DECISIÓN: MANTENERSE AL MARGEN (VOLATILIDAD MUERTA). ATR (${atr_value:.2f}) < Umbral Mínimo (${MIN_ATR_USD:.2f}).")
        return

    if atr_value > MAX_ATR_USD:
        logging.info(f"DECISIÓN: MANTENERSE AL MARGEN (VOLATILIDAD EXTREMA). ATR (${atr_value:.2f}) > Umbral Máximo (${MAX_ATR_USD:.2f}).")
        return
    # ----------------------------------------------------

    # 2. Lógica de Decisión (Identificación de Dirección) - ÚNICA VEZ
    if bias_score > dynamic_threshold:
        direction = "LONG (COMPRA)"
    elif bias_score < -dynamic_threshold:
        direction = "SHORT (VENTA)"
    else:
        direction = "NEUTRAL"
        logging.info(f"DECISIÓN: MANTENERSE AL MARGEN (SESGO NEUTRO). Umbral requerido: ${dynamic_threshold:.2f}")
        return
        
    # 3. Calcular los niveles de salida 
    stop_loss, take_profit = calculate_exit_levels(entry_price, atr_value, direction)

    # 4. Simulación y Reporte de la Orden
    amount_usd = 100.0  # Invertir 100 USD
    amount_base = amount_usd / entry_price
    
    logging.info(f"DECISIÓN: INICIAR {direction}")
    logging.info("-" * 50)
    logging.info("--- ORDEN SIMULADA ---")
    logging.info(f"Activo: {symbol}")
    logging.info(f"Dirección: {direction}")
    logging.info(f"Score (GR): ${bias_score:.2f}")
    logging.info(f"Precio Entrada: ${entry_price:.2f}")
    logging.info(f"Cantidad Base: {amount_base:.5f} {symbol.split('/')[0]}")
    logging.info(f"Volatilidad (ATR): ${atr_value:.2f}")
    logging.info(f"STOP LOSS (SL): ${stop_loss:.2f}")
    logging.info(f"TAKE PROFIT (TP): ${take_profit:.2f}")
    logging.info("-" * 50)

    # 5. Guardar la posición
    # Crear instancia Position para mayor consistencia
    ot = open_time
    # Convertir pandas.Timestamp a datetime si es necesario
    try:
        import pandas as _pd
        if isinstance(ot, _pd.Timestamp):
            ot = ot.to_pydatetime()
    except Exception:
        pass

    pos_obj = Position(
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        amount_base=amount_base,
        stop_loss=stop_loss,
        take_profit=take_profit,
        status='OPEN',
        open_time=ot
    )
    OPEN_POSITIONS.append(pos_obj)

    save_open_positions()


def print_final_trade_report(custom_prefix=None):
    """Envía un reporte analítico de nivel profesional a Telegram."""
    global CLOSED_TRADES
    
    if not CLOSED_TRADES:
        msg = "📊 *REPORTE DE JORNADA*\nNo hay operaciones cerradas todavía."
        bot.send_message(CHAT_ID, msg)
        return
        
    df_results = pd.DataFrame(CLOSED_TRADES)
    
    # Cálculos Métricos
    total_pnl = df_results['pnl_usd'].sum()
    wins = df_results[df_results['pnl_usd'] > 0]
    losses = df_results[df_results['pnl_usd'] <= 0]
    
    gross_profit = wins['pnl_usd'].sum()
    gross_loss = abs(losses['pnl_usd'].sum())
    profit_factor = gross_profit / gross_loss if gross_loss != 0 else float('inf')
    win_rate = (len(wins) / len(df_results)) * 100

    # Construcción del mensaje
    report_msg = custom_prefix if custom_prefix else "📊 *AUDITORÍA DE DISCIPLINA AUTOMATIZADA*\n"
    report_msg += "--------------------------------------------------\n"
    
    for _, row in df_results.iterrows():
        icon = "✅" if row['pnl_usd'] > 0 else "❌"
        # Mostramos el símbolo y el motivo de salida
        report_msg += f"{icon} *{row['symbol']}* | {row['exit_reason']}\n"
        report_msg += f"      PnL: `${row['pnl_usd']:.2f}`\n"
    
    report_msg += "--------------------------------------------------\n"
    report_msg += f"✅ *Ganados:* {len(wins)}  |  ❌ *Perdidos:* {len(losses)}\n"
    report_msg += f"🎯 *Win Rate:* `{win_rate:.2f}%` \n"
    report_msg += f"📈 *Profit Factor:* `{profit_factor:.2f}`\n"
    report_msg += f"💰 *PNL TOTAL:* `${total_pnl:.2f}`\n"
    report_msg += "--------------------------------------------------\n"
    
    # Si hay posiciones abiertas actualmente, avisamos
    if OPEN_POSITIONS:
        report_msg += f"⚠️ _Aviso: Hay {len(OPEN_POSITIONS)} posiciones aún abiertas._\n"
        
    report_msg += "🤖 _Ejecución 100% algorítmica._"

    bot.send_message(CHAT_ID, report_msg, parse_mode='Markdown')

def main():
    # ---------------------------------------------
    # 1. PARAMETRIZACIÓN GLOBAL (¡FIJADA!)
    # ---------------------------------------------
    TARGET_ASSETS = [
        'BTC/USD', 'ADA/USD', 'XRP/USD', 'SOL/USD', 
        'ETH/USD', 'LTC/USD', 'DOT/USD', 'BCH/USD', 'UNI/USD', 'LINK/USD'
    ]
    OPTIMAL_ATR_MULTIPLIER = 0.05 
    TIME_FRAME = '1h'
    HOURS_TO_ANALYZE = 50 

    # Limpieza necesaria
    global CLOSED_TRADES, OPEN_POSITIONS
    CLOSED_TRADES = []
    
    kraken = initialize_kraken_exchange()
    if not kraken:
        logging.error("Fallo la inicialización de Kraken. Deteniendo el proceso.")
        return

    # NUEVO: Verificación de Autenticación (Moviendo la lógica del if __name__ == '__main__':)
    try:
        balance = kraken.fetch_balance()
        logging.info("Autenticación exitosa. Saldo cargado.")
    except Exception as e:
        logging.error(f"Error CRÍTICO de autenticación: {e}. El bot no puede operar. Deteniendo.")
        return

    # =========================================================
    # --- SIMULACIÓN DE EJECUCIÓN LIVE ---
    # =========================================================

    load_open_positions()

    # [MODULO 1: APERTURA DE POSICIONES]
    # Este módulo se ejecutaría solo una vez al día (ej: 14:00 UTC)
    logging.info(f"[MODULO 1] INICIANDO APERTURA (Multiplicador ATR: {OPTIMAL_ATR_MULTIPLIER:.2f})")
    
    for symbol in TARGET_ASSETS:
        execute_live_trade(
            kraken, 
            symbol=symbol, 
            atr_multiplier=OPTIMAL_ATR_MULTIPLIER,
            hours_to_analyze=HOURS_TO_ANALYZE
        )

    # [MODULO 2: MONITOREO Y CIERRE REAL]
    logging.info("[MODULO 2] OBTENIENDO PRECIOS DE CIERRE REALES DE KRAKEN...")
    
    real_current_prices = {}
    for symbol in TARGET_ASSETS:
        try:
            ticker = kraken.fetch_ticker(symbol)
            real_current_prices[symbol] = ticker['last'] # Captura el último precio real
            logging.info(f"Precio capturado: {symbol} -> ${real_current_prices[symbol]}")
        except Exception as e:
            logging.error(f"Error al capturar precio real de {symbol}: {e}")

    # Ahora monitoreamos y cerramos con datos REALES del mercado
    monitor_and_close_positions(real_current_prices, kraken) 

    # [MODULO 3: REPORTE FINAL]
    print_final_trade_report()


def initialize_kraken_exchange():
    try:
        exchange = ccxt.kraken({
            'apiKey': os.getenv('KRAKEN_API_KEY'),
            'secret': os.getenv('KRAKEN_SECRET'),
            'enableRateLimit': True,
        })
        return exchange
    except Exception as e:
        logging.error(f"Error Kraken: {e}")
        return None


# --- 5. BUCLE PRINCIPAL (El corazón del Bot) ---
if __name__ == "__main__":
    kraken = initialize_kraken_exchange()
    if kraken:
        load_open_positions()
        
        # Iniciar Telegram en segundo plano
        threading.Thread(target=lambda: bot.polling(none_stop=True), daemon=True).start()
        
        logging.info("🛡️ SISTEMA EN STANDBY. Esperando /start_trading...")
        
        while True:
            if trading_active:
                try:
                    run_trading_cycle(kraken)
                except Exception as e:
                    logging.error(f"Error crítico en el ciclo: {e}")
            
            # Esperar 1 minuto entre chequeos
            time.sleep(900)