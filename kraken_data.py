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

@bot.message_handler(commands=['stop_trading'])
def handle_stop(message):
    global trading_active
    trading_active = False
    bot.reply_to(message, "🛑 *SISTEMA DETENIDO*")

def run_initial_cycle():
    """Ejecuta execute_live_trade para cada activo del TARGET_ASSETS global"""
    if not trading_active: return
    for symbol in TARGET_ASSETS:
        try:
            # Aquí llamamos a tu función original del bloc de notas
            execute_live_trade(kraken, symbol, OPTIMAL_ATR_MULTIPLIER, '1h', HOURS_TO_ANALYZE)
        except Exception as e:
            logging.error(f"Error en {symbol}: {e}")

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
    """
    Ejecuta la estrategia de Sesgo de Tiempo en un activo específico. 
    Obtiene los datos recientes (last N hours) para calcular el ATR y el Sesgo.
    """
    
    logging.info(f"--- [ LIVE TRADE: {symbol} ] Analizando...")
    
    # 1. Obtener Datos Recientes (Usando la nueva función de límite)
    historical_data = fetch_recent_data(kraken, symbol, timeframe, limit=hours_to_analyze)

    if historical_data is None or historical_data.empty:
        logging.warning(f"No hay datos recientes para {symbol}. Saltando.")
        return

    # 2. Análisis del Sesgo de Tiempo
    processed_data = preprocess_data_for_time_bias(historical_data)
    data_with_zones = mark_kill_zones(processed_data)
    
    # Calcula el puntaje de sesgo (Gross Return Score)
    time_bias_score = analyze_gross_return(data_with_zones)

    # 3. Decisión y Ejecución
    # Llama a la función de simulación que contiene toda la lógica optimizada
    execute_trade_simulation(
        symbol, 
        time_bias_score, 
        atr_multiplier, 
        historical_data
    )
    
    # NOTA: Opcionalmente, puedes eliminar las llamadas a analyze_time_bias y analyze_all_hours
    # de este punto para que la ejecución en vivo sea más limpia y rápida, 
    # ya que solo son útiles para el reporte y análisis en backtesting.
    
    logging.info(f"{symbol} | Sesgo: {time_bias_score:.2f} | Decisión Registrada.")    
      

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
KILL_ZONE_START = 14
KILL_ZONE_END = 18

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
    Monitorea posiciones abiertas contra SL/TP/Time Exit.
    current_price_data: Diccionario con precios actuales (simulados o reales).
    exchange: Instancia de CCXT para obtener la hora y potencialmente ejecutar órdenes reales.
    """
    global OPEN_POSITIONS, CLOSED_TRADES 

    # 1. Obtener la hora actual UTC
    # Usamos la hora local de la máquina y la convertimos a UTC
    now_utc = datetime.now(pytz.utc)
    current_utc_hour = now_utc.hour
    
    # Si estamos dentro de la Kill Zone, no deberíamos aplicar Time Exit todavía.
    # El Time Exit solo aplica DESPUÉS de la hora de cierre de la Kill Zone.
    time_exit_allowed = (current_utc_hour >= KILL_ZONE_END)
    
    if time_exit_allowed:
        logging.info(f"--- [ CIERRE POR TIEMPO ACTIVO ] --- Hora actual: {now_utc.strftime('%H:%M:%S')} UTC")
    else:
        logging.info(f"--- [ MONITOREO SL/TP ] --- Hora actual: {now_utc.strftime('%H:%M:%S')} UTC")


    # Recorrer las posiciones de atrás hacia adelante para eliminar sin problemas
    for i in range(len(OPEN_POSITIONS) - 1, -1, -1):
        pos = OPEN_POSITIONS[i]
        # Aceptar tanto `Position` como `dict` en la lista de posiciones.
        # Si es `Position`, convertir y reemplazar el elemento en la lista
        # para mantener consistencia con el resto del código que usa dicts.
        if isinstance(pos, Position):
            pos = OPEN_POSITIONS[i] = pos.to_dict()
        symbol = pos['symbol']
        
        current_price = current_price_data.get(symbol)
        
        if current_price is None:
            logging.warning(f"ADVERTENCIA: Precio actual no encontrado para {symbol}. Saltando monitoreo.")
            continue
            
        exit_reason = None
        close_price = None 

        # 2. Lógica de CIERRE por SL/TP (Prioridad Máxima)
        if pos['direction'] == 'LONG (COMPRA)':
            if current_price >= pos['take_profit']:
                exit_reason = "TAKE PROFIT (TP)"
                close_price = pos['take_profit'] # Usar nivel fijo
            elif current_price <= pos['stop_loss']:
                exit_reason = "STOP LOSS (SL)"
                close_price = pos['stop_loss'] # Usar nivel fijo
        
        elif pos['direction'] == 'SHORT (VENTA)':
            if current_price <= pos['take_profit']: 
                exit_reason = "TAKE PROFIT (TP)"
                close_price = pos['take_profit'] # Usar nivel fijo
            elif current_price >= pos['stop_loss']: 
                exit_reason = "STOP LOSS (SL)"
                close_price = pos['stop_loss'] # Usar nivel fijo

        # 3. Lógica de CIERRE por Tiempo (Time Exit)
        # Solo se ejecuta si no se ha cerrado por SL/TP y el tiempo ha expirado
        if exit_reason is None and time_exit_allowed:
            exit_reason = "TIME EXIT (KZ EXPIRÓ)"
            close_price = current_price # Cerrar al precio de mercado (simulado)
            
        # 4. Ejecución del Cierre y Registro
        if exit_reason:
            
            # Calcular PnL (Ganancia/Pérdida)
            pnl_usd = (close_price - pos['entry_price']) * pos['amount_base']
            
            # Si fue un SHORT, el cálculo debe ser inverso 
            if pos['direction'] == 'SHORT (VENTA)':
                pnl_usd = -pnl_usd 

            pnl_status = "GANANCIA" if pnl_usd > 0 else "PÉRDIDA"
            
            logging.info(f"CIERRE {symbol} | Motivo: {exit_reason} | PnL: ${pnl_usd:.2f} ({pnl_status})")
            
            # Mover la posición a la lista de cerradas y eliminar de la lista abierta
            pos['status'] = 'CLOSED'
            pos['exit_price'] = close_price 
            pos['exit_reason'] = exit_reason
            pos['pnl_usd'] = pnl_usd 
            
            CLOSED_TRADES.append(OPEN_POSITIONS.pop(i))

            save_open_positions()
            
    if not OPEN_POSITIONS:
        logging.info("NO HAY POSICIONES ABIERTAS PENDIENTES")
    else:
        logging.info(f"{len(OPEN_POSITIONS)} POSICIONES ABIERTAS PENDIENTES")


# ----------------------------------------------------
# NUEVA FUNCIÓN: SIMULACIÓN DE ENTRADA DE TRADING
# ----------------------------------------------------

def execute_trade_simulation(symbol, bias_score, atr_multiplier_value, historical_data): 
    """
    Simula una orden de mercado con cálculo de Stop Loss y Take Profit.
    Ahora incluye filtro de robustez ATR Mín/Máx.
    """
    global OPEN_POSITIONS

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


def print_final_trade_report():
    """Imprime un reporte analítico de nivel profesional para el grupo."""
    global CLOSED_TRADES
    if not CLOSED_TRADES:
        logging.info("--- REPORTE: Sin operaciones cerradas en este ciclo ---")
        return
        
    df_results = pd.DataFrame(CLOSED_TRADES)
    
    # Cálculos Métricos
    total_pnl = df_results['pnl_usd'].sum()
    wins = df_results[df_results['pnl_usd'] > 0]['pnl_usd']
    losses = df_results[df_results['pnl_usd'] <= 0]['pnl_usd']
    
    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())
    profit_factor = gross_profit / gross_loss if gross_loss != 0 else float('inf')
    win_rate = (len(wins) / len(df_results)) * 100

    # Formateo del Reporte para el Grupo
    logging.info("=" * 60)
    logging.info("📊 AUDITORÍA DE DISCIPLINA AUTOMATIZADA")
    logging.info("=" * 60)
    logging.info(df_results[['symbol', 'direction', 'exit_reason', 'pnl_usd']].to_string(index=False))
    logging.info("-" * 60)
    logging.info(f"✅ Trades Ganados: {len(wins)} | ❌ Trades Perdidos: {len(losses)}")
    logging.info(f"🎯 Win Rate: {win_rate:.2f}%")
    logging.info(f"📈 Profit Factor: {profit_factor:.2f}")
    logging.info(f"💰 PNL TOTAL DE LA JORNADA: ${total_pnl:.2f}")
    logging.info("=" * 60)
    logging.info("Nota: Ejecución 100% algorítmica sin intervención humana.")

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
            time.sleep(60)