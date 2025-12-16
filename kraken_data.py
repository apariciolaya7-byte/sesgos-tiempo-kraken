import ccxt
import os
from dotenv import load_dotenv
import pandas as pd
import pytz
from datetime import datetime
import time
import ta.volatility

# REGISTRO GLOBAL DE POSICIONES ABIERTAS (Manejo de estado)
OPEN_POSITIONS = []
CLOSED_TRADES = [] # <-- AÑADIDO: Para guardar los resultados de PnL
# NUEVO: Para almacenar los resultados totales de cada corrida de optimización
OPTIMIZATION_RESULTS = []


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

# 3. Prueba la conexión (opcional, pero útil)
if __name__ == '__main__':
    kraken = initialize_kraken_exchange()
    if kraken:
        # ccxt tiene una función para verificar si la autenticación funciona
        # Esto usará un 'endpoint' privado y requiere que las claves sean válidas.
        try:
             # Por ejemplo, obteniendo información de la cuenta (solo si las claves son válidas)
            balance = kraken.fetch_balance()
            print("✅ Autenticación exitosa. Saldo cargado.")
        except Exception as e:
            # Si el error es por claves inválidas, te lo indicará.
            print(f"⚠️ Atención: Las claves de API son incorrectas o no tienen permisos.")
            print(f"Error detallado: {e}")


MAX_LIMIT = 720 

def fetch_historical_data(exchange, symbol='BTC/USD', timeframe='1h', start_date_str='YYYY-MM-DD'):
    """
    Descarga datos OHLCV históricos en bloques hasta la fecha de inicio.
    """
    
    # 1. Convertir fecha de inicio (Ej: '2025-06-01') a Timestamp UNIX (en milisegundos)
    try:
        since_timestamp = exchange.parse8601(start_date_str + 'T00:00:00Z')
    except Exception:
        print("❌ Error: Formato de fecha de inicio inválido. Use 'YYYY-MM-DD'.")
        return None
    
    all_ohlcv = []
    current_timestamp = since_timestamp # Empezamos a buscar desde la fecha de inicio

    print(f"Iniciando descarga histórica desde: {start_date_str}...")

    # Bucle para descargar los datos por bloques
    while True:
        try:
            # 2. Hacemos la llamada con un límite de velas
            ohlcv_chunk = exchange.fetch_ohlcv(
                symbol, 
                timeframe, 
                since=current_timestamp, 
                limit=MAX_LIMIT
            )

            # Si el chunk está vacío o el último bloque es muy pequeño, hemos terminado
            if not ohlcv_chunk or len(ohlcv_chunk) < MAX_LIMIT:
                all_ohlcv.extend(ohlcv_chunk)
                print(f"✅ Descarga finalizada. Total de velas obtenidas: {len(all_ohlcv)}")
                break

            # 3. Guardar el bloque y actualizar el timestamp
            all_ohlcv.extend(ohlcv_chunk)
            
            # El 'since' para la siguiente llamada debe ser la hora de la ÚLTIMA vela en este chunk
            current_timestamp = ohlcv_chunk[-1][0] + 1 # +1 ms para no repetir la última vela

            print(f"  -> Bloque descargado. Total actual: {len(all_ohlcv)}. Última vela: {datetime.fromtimestamp(current_timestamp / 1000).strftime('%Y-%m-%d %H:%M')}")
            
            # ¡CRÍTICO! Esperar para evitar el límite de frecuencia de la API.
            time.sleep(exchange.rateLimit / 1000) 

        except Exception as e:
            print(f"❌ Error durante la descarga: {e}. Reintentando en 5 segundos...")
            time.sleep(5)

    # 4. Compilación de Datos en un único DataFrame
    headers = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(all_ohlcv, columns=headers)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    return df
        
        

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

def analyze_time_bias(df):
    """
    Calcula el volumen promedio y el rango promedio dentro y fuera de la Kill Zone.
    """
    # Agrupación por la nueva columna booleana:

    bias_analysis = df.groupby('is_kill_zone').agg(
        avg_volume=('volume', 'mean'),
        avg_range=('candle_range', 'mean')
    )
    # Renombrar los índices para mayor claridad
    bias_analysis = bias_analysis.rename(index={
        True: 'KILL_ZONE (Alta Liquidez)',
        False: 'LOW_LIQUIDITY (Fuera de Zona)'
    })
    
    print("\n📊 Análisis de Sesgo de Tiempo (Basado en la muestra de 24h):")
    print("---------------------------------------------------------")
    print(bias_analysis)
    print("---------------------------------------------------------")
    
    return bias_analysis

def analyze_all_hours(df, symbol='BTC/USD'): # <--- AQUÍ DEBE RECIBIR EL PARÁMETRO
    """Calcula el volumen y rango promedio para CADA hora del día y guarda el resultado."""
    hourly_analysis = df.groupby('hour_utc').agg(
        avg_volume=('volume', 'mean'),
        avg_range=('candle_range', 'mean'),
        count=('timestamp', 'size')
    ).sort_values(by='avg_volume', ascending=False)
    
    # -----------------
    # AÑADIR EXPORTACIÓN CSV
    # -----------------
    # Usa el parámetro 'symbol' que recibe la función
    report_filename = f'{symbol.replace("/", "_")}_time_bias_hourly_analysis.csv' 
    hourly_analysis.to_csv(report_filename)
    print(f"\n✅ Reporte de Análisis por Hora guardado en: {report_filename}")

    print("\n📊 Análisis Detallado por Hora (UTC):")
    print(hourly_analysis.head(5)) 
    print("-" * 40)
    
    peak_hour = hourly_analysis.iloc[0].name
    print(f"Hora Pico de Volumen Real (UTC): {peak_hour}:00")
    
    return hourly_analysis

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


def monitor_and_close_positions(current_price_data):
    """
    Simula la comprobación de posiciones abiertas contra SL/TP/Time Exit, y calcula PnL.
    current_price_data: Diccionario con precios actuales (ej: {'BTC/USD': 87087.04, ...})
    """
    global OPEN_POSITIONS, CLOSED_TRADES # Asegurarse de que CLOSED_TRADES sea global

    print("\n--- INICIANDO MONITOREO DE POSICIONES ---")
    
    # Recorrer las posiciones de atrás hacia adelante para eliminar sin problemas
    for i in range(len(OPEN_POSITIONS) - 1, -1, -1):
        pos = OPEN_POSITIONS[i]
        symbol = pos['symbol']
        
        current_price = current_price_data.get(symbol)
        
        if current_price is None:
            print(f"!!! ADVERTENCIA: Precio actual no encontrado para {symbol}. Saltando monitoreo.")
            continue
            
        exit_reason = None
        close_price = None # Usaremos esta variable para el cálculo de PnL

        # Lógica de CIERRE LONG
        if pos['direction'] == 'LONG (COMPRA)':
            if current_price >= pos['take_profit']:
                exit_reason = "TAKE PROFIT (TP)"
                close_price = pos['take_profit'] # ¡USAR NIVEL FIJO!
            elif current_price <= pos['stop_loss']:
                exit_reason = "STOP LOSS (SL)"
                close_price = pos['stop_loss'] # ¡USAR NIVEL FIJO!
        
        # Lógica de CIERRE SHORT
        elif pos['direction'] == 'SHORT (VENTA)':
            if current_price <= pos['take_profit']: 
                exit_reason = "TAKE PROFIT (TP)"
                close_price = pos['take_profit'] # ¡USAR NIVEL FIJO!
            elif current_price >= pos['stop_loss']: 
                exit_reason = "STOP LOSS (SL)"
                close_price = pos['stop_loss'] # ¡USAR NIVEL FIJO!

        # **************************************************
        # NUEVO: LÓGICA DE CIERRE POR TIEMPO (TIME EXIT)
        # **************************************************
        # Si no cerró por precio, se cierra por tiempo al precio actual simulado
        if exit_reason is None:
            exit_reason = "TIME EXIT (END OF KZ)"
            close_price = current_price # Usar el precio simulado como precio de cierre
            
        # Si hay una razón de salida (TP, SL o Time Exit), cerrar la posición
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



# Modificamos la función existente para que no imprima, sino que devuelva las métricas
def calculate_metrics():
    """Calcula las métricas de rendimiento de la corrida actual."""
    global CLOSED_TRADES
    if not CLOSED_TRADES:
        return 0, 0, 0.0, 0 # Trades, Wins, Win Rate, PNL
        
    df_results = pd.DataFrame(CLOSED_TRADES)
    total_pnl = df_results['pnl_usd'].sum()
    win_trades = len(df_results[df_results['pnl_usd'] > 0])
    total_trades = len(df_results)
    win_rate = (win_trades / total_trades) * 100 if total_trades > 0 else 0
    
    return total_trades, win_trades, win_rate, total_pnl

# Función para registrar la corrida
def record_optimization_result(multiplier):
    """Guarda los resultados de la corrida actual en el registro global."""
    global OPTIMIZATION_RESULTS
    total_trades, win_trades, win_rate, total_pnl = calculate_metrics()
    
    OPTIMIZATION_RESULTS.append({
        'ATR_Multiplier': multiplier,
        'Total_Trades': total_trades,
        'Win_Trades': win_trades,
        'Win_Rate': f"{win_rate:.2f}%",
        'PNL_Total': round(total_pnl, 2)
    })

# Función para imprimir el reporte final de todas las corridas
def print_optimization_report():
    """Imprime una tabla comparativa de los resultados de optimización."""
    global OPTIMIZATION_RESULTS
    
    if not OPTIMIZATION_RESULTS:
        print("\n=== REPORTE DE OPTIMIZACIÓN FINAL ===")
        print("No se registraron corridas de backtesting.")
        return
        
    df_report = pd.DataFrame(OPTIMIZATION_RESULTS)
    
    # Encontrar el mejor resultado basado en PNL Total
    best_result = df_report.loc[df_report['PNL_Total'].idxmax()]
    
    print("\n\n=======================================================")
    print("=== REPORTE DE OPTIMIZACIÓN FINAL (ATR Multiplier) ===")
    print("=======================================================")
    print(df_report.to_markdown(index=False))
    print("-------------------------------------------------------")
    print(f"🥇 MEJOR CORRIDA (PNL): Multiplicador {best_result['ATR_Multiplier']:.2f} con PNL ${best_result['PNL_Total']:.2f}")
    print("=======================================================")  


def main():
    # ---------------------------------------------
    # 1. PARAMETRIZACIÓN GLOBAL
    # ---------------------------------------------
    TARGET_ASSETS = [
        'BTC/USD', 'ADA/USD', 'XRP/USD', 'SOL/USD', 
        'ETH/USD', 'LTC/USD', 'DOT/USD', 'BCH/USD', 'UNI/USD', 'LINK/USD'
    ]
    TIME_FRAME = '1h'
    START_DATE = '2025-06-14' 
    
    # NUEVO: Rango de valores a probar para el multiplicador de ATR (Filtro)
    ATR_MULTIPLIERS_TO_TEST = [0.05, 0.10, 0.15, 0.20] 
    
    # ---------------------------------------------
    
    kraken = initialize_kraken_exchange()
    if not kraken:
        print("Fallo la inicialización de Kraken. Deteniendo el proceso.")
        return

    # --- INICIO DEL BUCLE DE OPTIMIZACIÓN ---
    for multiplier in ATR_MULTIPLIERS_TO_TEST:
        
        # 1. Resetear el estado para esta corrida
        global OPEN_POSITIONS, CLOSED_TRADES
        OPEN_POSITIONS = []
        CLOSED_TRADES = [] 
        
        print(f"\n=======================================================")
        print(f"=== CORRIDA DE OPTIMIZACIÓN: ATR Multiplicador = {multiplier:.2f} ===")
        print(f"=======================================================\n")
        
        # 2. Bucle interno para ITERAR sobre cada activo (Lógica de Apertura)
        for symbol in TARGET_ASSETS:
            TARGET_SYMBOL = symbol # Asignación de símbolo
        
            # *************************************************************
            # * AHORA, TODA ESTA LÓGICA ESTÁ DENTRO DEL BUCLE INTERNO     *
            # *************************************************************
            
            print(f"\n--- [ {TARGET_SYMBOL} ] Iniciando análisis...")
            
            # 3. Descargar los datos OHLCV para el activo actual
            historical_data = fetch_historical_data(
                kraken, 
                symbol=TARGET_SYMBOL, 
                timeframe=TIME_FRAME, 
                start_date_str=START_DATE
            ) 
            
            if historical_data is not None and not historical_data.empty: 
                
                # 4. Procesamiento y Análisis
                processed_data = preprocess_data_for_time_bias(historical_data)
                data_with_zones = mark_kill_zones(processed_data)

                # 5. CÁLCULO DEL SESGO
                time_bias_score = analyze_gross_return(data_with_zones)

                # Impresión de sesgo...
                
                # 6. PASO CLAVE: Ejecutar la simulación
                execute_trade_simulation(
                    TARGET_SYMBOL, 
                    time_bias_score, 
                    multiplier, # <--- Correcto: usa el multiplicador actual
                    historical_data
                )

                # 7. Reporte de Consola y Generación de CSV
                analyze_time_bias(data_with_zones) 
                analyze_all_hours(processed_data, symbol=TARGET_SYMBOL)

            else:
                print(f"!!! ADVERTENCIA: No se pudo obtener datos históricos para {TARGET_SYMBOL}. Saltando.")
                
            print(f"--- [ {TARGET_SYMBOL} ] Análisis Finalizado.\n")
            
        # *************************************************************
        # * FIN DEL BUCLE INTERNO (for symbol in TARGET_ASSETS)       *
        # *************************************************************


        # 3. Simulación de Monitoreo y Cierre (CORREGIDO: Fuera del bucle interno)
        simulated_current_prices = {
           'BTC/USD': 87087.04, 'ETH/USD': 2856.80, 
           'SOL/USD': 122.27, 'BCH/USD': 552.59,
           'LTC/USD': 74.90, 'ADA/USD': 0.50,
           'XRP/USD': 0.55, 'DOT/USD': 6.00, 'UNI/USD': 10.00, 'LINK/USD': 15.00
        }

        print("--- INICIANDO MONITOREO DE POSICIONES ---")
        monitor_and_close_positions(simulated_current_prices)
            
        # 4. Registrar los resultados de la corrida 
        record_optimization_result(multiplier) 

    # --- FIN DEL BUCLE DE OPTIMIZACIÓN ---
    
    # 5. Reporte de Optimización Final (FUERA DE TODOS LOS BUCLES)
    print_optimization_report()          


if __name__ == '__main__':
    main()