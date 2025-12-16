import ccxt
import os
from dotenv import load_dotenv
import pandas as pd
import pytz
from datetime import datetime
import time
import ta.volatility
import json


# REGISTRO GLOBAL DE POSICIONES ABIERTAS (Manejo de estado)
OPEN_POSITIONS = []
CLOSED_TRADES = [] # <-- AÑADIDO: Para guardar los resultados de PnL


# 1. Cargar variables del archivo .env
load_dotenv()

# 2. Inicializar la conexión
def initialize_kraken_exchange():
    """Inicializa la instancia de Kraken usando las credenciales del entorno."""
    
    # Intenta inicializar el exchange con las credenciales, si existen
    try:
        exchange = ccxt.kraken({
            'apiKey': os.getenv('KRAKEN_API_KEY'),
            'secret': os.getenv('KRAKEN_SECRET'),
            'enableRateLimit': True, # Para evitar exceder los límites de la API
        })
        print("✅ Conexión a Kraken inicializada correctamente.")
        return exchange
    except Exception as e:
        print(f"❌ Error al inicializar Kraken: {e}")
        return None


# Nombre del archivo para guardar las posiciones abiertas
POSITIONS_FILE = 'open_positions.json'

def load_open_positions():
    """Carga las posiciones abiertas desde un archivo JSON al inicio."""
    global OPEN_POSITIONS
    try:
        if os.path.exists(POSITIONS_FILE):
            with open(POSITIONS_FILE, 'r') as f:
                data = json.load(f)
                
                # Convertir los timestamps cargados a objetos datetime si es necesario, 
                # o dejarlos como strings/timestamps para simplificar. 
                # Por ahora, los dejamos tal cual.
                OPEN_POSITIONS = data
                print(f"✅ {len(OPEN_POSITIONS)} Posiciones abiertas cargadas desde {POSITIONS_FILE}.")
                return
    except Exception as e:
        print(f"⚠️ Error al cargar posiciones: {e}. Iniciando con lista vacía.")
    
    OPEN_POSITIONS = []

def save_open_positions():
    """Guarda las posiciones abiertas en un archivo JSON."""
    global OPEN_POSITIONS
    try:
        with open(POSITIONS_FILE, 'w') as f:
            # Serializamos la lista de posiciones. Si tuviéramos objetos datetime,
            # deberíamos convertirlos a string antes de serializar.
            json.dump(OPEN_POSITIONS, f, indent=4)
        # print(f"✅ Estado de posiciones guardado en {POSITIONS_FILE}.")
    except Exception as e:
        print(f"❌ Error al guardar posiciones: {e}")            

        
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
            print(f"!!! No se obtuvieron datos recientes para {symbol}.")
            return None
            
        # 4. Compilación de Datos en un único DataFrame
        headers = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df = pd.DataFrame(ohlcv, columns=headers)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        return df
        
    except Exception as e:
        print(f"❌ Error al obtener datos recientes para {symbol}: {e}")
        return None
    

def execute_live_trade(kraken, symbol, atr_multiplier=0.05, timeframe='1h', hours_to_analyze=50):
    """
    Ejecuta la estrategia de Sesgo de Tiempo en un activo específico. 
    Obtiene los datos recientes (last N hours) para calcular el ATR y el Sesgo.
    """
    
    print(f"\n--- [ LIVE TRADE: {symbol} ] Analizando...")
    
    # 1. Obtener Datos Recientes (Usando la nueva función de límite)
    historical_data = fetch_recent_data(kraken, symbol, timeframe, limit=hours_to_analyze)

    if historical_data is None or historical_data.empty:
        print(f"!!! No hay datos recientes para {symbol}. Saltando.")
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
    
    print(f"--- {symbol} | Sesgo: {time_bias_score:.2f} | Decisión Registrada. ---")    
      

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
    
    print(f"✅ Datos pre-procesados. Zona horaria: {df.index.tz}")
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
    
    print("✅ Kill Zones marcadas en el DataFrame.")
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
    print("\n💰 Análisis de Retorno Bruto Promedio (por Vela):")
    print("-" * 50)
    
    # Manejo de NaN para evitar errores
    if pd.isna(kill_zone_gr):
        print(f"KILL ZONE (14:00 a 18:00 UTC): $nan (Movimiento promedio)")
        sesgo = "Neutro (Error de Cálculo o Datos insuficientes)."
        return 0.0 # Devolver 0.0 en caso de error para que el if/elif del main no falle
        
    # Continuación si no es NaN
    print(f"KILL ZONE (14:00 a 18:00 UTC): ${kill_zone_gr:.2f} (Movimiento promedio)")
    print(f"LOW LIQUIDITY (Otras Horas): ${low_liquidity_gr:.2f} (Movimiento promedio)")
    print("-" * 50)
    
    if kill_zone_gr > 0:
        sesgo = "Ligeramente Alcista (el precio tiende a subir)."
    elif kill_zone_gr < 0:
        sesgo = "Ligeramente Bajista (el precio tiende a bajar)."
    else:
        sesgo = "Neutro."
        
    print(f"Sesgo de Dirección en la KILL ZONE: {sesgo}")
    
    # DEVUELVE el indicador clave: Retorno Bruto de la Kill Zone
    return kill_zone_gr



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
        print(f"\n--- [ CIERRE POR TIEMPO ACTIVO ] --- Hora actual: {now_utc.strftime('%H:%M:%S')} UTC")
    else:
        print(f"\n--- [ MONITOREO SL/TP ] --- Hora actual: {now_utc.strftime('%H:%M:%S')} UTC")


    # Recorrer las posiciones de atrás hacia adelante para eliminar sin problemas
    for i in range(len(OPEN_POSITIONS) - 1, -1, -1):
        pos = OPEN_POSITIONS[i]
        symbol = pos['symbol']
        
        current_price = current_price_data.get(symbol)
        
        if current_price is None:
            print(f"!!! ADVERTENCIA: Precio actual no encontrado para {symbol}. Saltando monitoreo.")
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
            
            print(f"✅ CIERRE {symbol} | Motivo: {exit_reason} | PnL: ${pnl_usd:.2f} ({pnl_status})")
            
            # Mover la posición a la lista de cerradas y eliminar de la lista abierta
            pos['status'] = 'CLOSED'
            pos['exit_price'] = close_price 
            pos['exit_reason'] = exit_reason
            pos['pnl_usd'] = pnl_usd 
            
            CLOSED_TRADES.append(OPEN_POSITIONS.pop(i))

            save_open_positions()
            
    if not OPEN_POSITIONS:
        print("--- NO HAY POSICIONES ABIERTAS PENDIENTES ---")
    else:
        print(f"--- {len(OPEN_POSITIONS)} POSICIONES ABIERTAS PENDIENTES ---")


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
        print(f"!!! ERROR al calcular ATR/Precios para {symbol}: {e}")
        return
    
    # ----------------------------------------------------
    # NUEVO FILTRO DE ROBUSTEZ: ATR Mínimo y Máximo
    # ----------------------------------------------------
    # Se establecen límites de sentido común para evitar trades en volatilidad nula o extrema.
    MIN_ATR_USD = 0.05  
    MAX_ATR_USD = 100.0 

    if atr_value < MIN_ATR_USD:
        print(f"🛑 DECISIÓN: MANTENERSE AL MARGEN (VOLATILIDAD MUERTA). ATR (${atr_value:.2f}) < Umbral Mínimo (${MIN_ATR_USD:.2f}).")
        return

    if atr_value > MAX_ATR_USD:
        print(f"🛑 DECISIÓN: MANTENERSE AL MARGEN (VOLATILIDAD EXTREMA). ATR (${atr_value:.2f}) > Umbral Máximo (${MAX_ATR_USD:.2f}).")
        return
    # ----------------------------------------------------

    # 2. Lógica de Decisión (Identificación de Dirección) - ÚNICA VEZ
    if bias_score > dynamic_threshold:
        direction = "LONG (COMPRA)"
    elif bias_score < -dynamic_threshold:
        direction = "SHORT (VENTA)"
    else:
        direction = "NEUTRAL"
        print(f"🛑 DECISIÓN: MANTENERSE AL MARGEN (SESGO NEUTRO). Umbral requerido: ${dynamic_threshold:.2f}")
        return
        
    # 3. Calcular los niveles de salida 
    stop_loss, take_profit = calculate_exit_levels(entry_price, atr_value, direction)

    # 4. Simulación y Reporte de la Orden
    amount_usd = 100.0  # Invertir 100 USD
    amount_base = amount_usd / entry_price
    
    print(f"💰 DECISIÓN: INICIAR {direction}")
    print("-" * 50)
    print(f"--- ORDEN SIMULADA ---")
    print(f" Activo: {symbol}")
    print(f" Dirección: {direction}")
    print(f" Score (GR): ${bias_score:.2f}")
    print(f" Precio Entrada: ${entry_price:.2f}")
    print(f" Cantidad Base: {amount_base:.5f} {symbol.split('/')[0]}")
    print(f" Volatilidad (ATR): ${atr_value:.2f}")
    print(f" ** STOP LOSS (SL): ${stop_loss:.2f} **")
    print(f" ** TAKE PROFIT (TP): ${take_profit:.2f} **")
    print("-" * 50)

    # 5. Guardar la posición
    new_position = {
        'symbol': symbol,
        'direction': direction,
        'entry_price': entry_price,
        'amount_base': amount_base,
        'stop_loss': stop_loss,
        'take_profit': take_profit,
        'status': 'OPEN',
        'open_time': open_time 
    }
    OPEN_POSITIONS.append(new_position)

    save_open_positions()


# Función para imprimir el reporte final de todas las corridas
def print_final_trade_report():
    """Imprime los trades cerrados para la corrida 0.05."""
    global CLOSED_TRADES
    if not CLOSED_TRADES:
        print("No se cerraron trades durante la ejecución.")
        return
        
    df_results = pd.DataFrame(CLOSED_TRADES)
    total_pnl = df_results['pnl_usd'].sum()
    
    print("\n--- REPORTE FINAL DE TRADES CERRADOS ---")
    print(df_results[['symbol', 'direction', 'entry_price', 'exit_price', 'exit_reason', 'pnl_usd']].to_markdown(index=False))
    print(f"PNL TOTAL DE LA JORNADA: ${total_pnl:.2f}")
    print("------------------------------------------")

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
        print("Fallo la inicialización de Kraken. Deteniendo el proceso.")
        return

    # NUEVO: Verificación de Autenticación (Moviendo la lógica del if __name__ == '__main__':)
    try:
         balance = kraken.fetch_balance()
         print("✅ Autenticación exitosa. Saldo cargado.")
    except Exception as e:
         print(f"⚠️ ¡Error CRÍTICO de autenticación! El bot no puede operar. Deteniendo.")
         return

    # =========================================================
    # --- SIMULACIÓN DE EJECUCIÓN LIVE ---
    # =========================================================

    load_open_positions()

    # [MODULO 1: APERTURA DE POSICIONES]
    # Este módulo se ejecutaría solo una vez al día (ej: 14:00 UTC)
    print(f"\n[MODULO 1] INICIANDO APERTURA (Multiplicador ATR: {OPTIMAL_ATR_MULTIPLIER:.2f})")
    
    for symbol in TARGET_ASSETS:
        execute_live_trade(
            kraken, 
            symbol=symbol, 
            atr_multiplier=OPTIMAL_ATR_MULTIPLIER,
            hours_to_analyze=HOURS_TO_ANALYZE
        )

    # [MODULO 2: MONITOREO Y CIERRE]
    # En un entorno real, esto se ejecutaría en un bucle cada 5-10 minutos.
    print("\n[MODULO 2] SIMULANDO MONITOREO Y CIERRE (Se asume la hora de cierre de KZ)")
    
    # Usamos los precios simulados para la prueba final (simulando precios de las 18:00 UTC)
    simulated_current_prices = {
       'BTC/USD': 87087.04, 'ETH/USD': 2856.80, 
       'SOL/USD': 122.27, 'BCH/USD': 552.59,
       'LTC/USD': 74.90, 'ADA/USD': 0.50,
       'XRP/USD': 0.55, 'DOT/USD': 6.00, 'UNI/USD': 10.00, 'LINK/USD': 15.00
    }
    
    # LLAMAMOS A LA FUNCIÓN CON EL EXCHANGE REAL, aunque los precios sean simulados
    monitor_and_close_positions(simulated_current_prices, kraken) 

    # [MODULO 3: REPORTE FINAL]
    print_final_trade_report()        
