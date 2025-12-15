import ccxt
import os
from dotenv import load_dotenv
import pandas as pd
import pytz
from datetime import datetime
import time

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

def analyze_all_hours(df):
    """Calcula el volumen y rango promedio para CADA hora del día y guarda el resultado."""
    hourly_analysis = df.groupby('hour_utc').agg(
        avg_volume=('volume', 'mean'),
        avg_range=('candle_range', 'mean'),
        count=('timestamp', 'size')
    ).sort_values(by='avg_volume', ascending=False)
    
    # -----------------
    # AÑADIR EXPORTACIÓN CSV
    # -----------------
    report_filename = 'time_bias_hourly_analysis.csv'
    hourly_analysis.to_csv(report_filename)
    print(f"\n✅ Reporte de Análisis por Hora guardado en: {report_filename}")

    print("\n📊 Análisis Detallado por Hora (UTC):")
    print(hourly_analysis.head(5)) 
    print("-" * 40)
    
    peak_hour = hourly_analysis.iloc[0].name
    print(f"Hora Pico de Volumen Real (UTC): {peak_hour}:00")
    
    return hourly_analysis # Retornar el DF para su uso si es necesario

def analyze_gross_return(df):
    """
    Calcula el movimiento promedio (Open a Close) para estimar el Retorno Bruto promedio.
    """
    # 1. Calcular el movimiento de la vela (Cierre - Apertura)
    df['candle_return'] = df['close'] - df['open']
    
    # 2. Agrupar el retorno por Kill Zone
    return_analysis = df.groupby('is_kill_zone')['candle_return'].mean()
    
    print("\n💰 Análisis de Retorno Bruto Promedio (por Vela):")
    print("--------------------------------------------------")
    
    # El valor es el movimiento promedio de la vela en el tiempo de la Kill Zone
    kill_zone_return = return_analysis.get(True, 0)
    low_liquidity_return = return_analysis.get(False, 0)
    
    print(f"KILL ZONE ({KILL_ZONE_START:02d}:00 a {KILL_ZONE_END:02d}:00 UTC): ${kill_zone_return:.2f} (Movimiento promedio)")
    print(f"LOW LIQUIDITY (Otras Horas): ${low_liquidity_return:.2f} (Movimiento promedio)")
    print("--------------------------------------------------")
    
    # Evaluar el sesgo de dirección: ¿sube o baja?
    if kill_zone_return > 0:
        print("Sesgo de Dirección en la KILL ZONE: Ligeramente Alcista (el precio tiende a subir).")
    elif kill_zone_return < 0:
        print("Sesgo de Dirección en la KILL ZONE: Ligeramente Bajista (el precio tiende a bajar).")
    else:
        print("Sesgo de Dirección en la KILL ZONE: Neutro.")

if __name__ == '__main__':
    # Define la fecha de inicio para el backtesting (Aproximadamente 6 meses de datos)
    # Fecha: 14 de junio de 2025 (asumiendo que hoy es 14 de diciembre de 2025)
    START_DATE = '2025-06-14' 
    
    # Paso 1: Inicializar la conexión
    kraken = initialize_kraken_exchange()

    if kraken:
        # Paso 2: Descargar los datos OHLCV (¡Ahora llamando a la función histórica!)
        historical_data = fetch_historical_data(kraken, start_date_str=START_DATE) 
        
        # Paso 3: Verificar que la descarga fue exitosa antes de continuar
        if historical_data is not None and not historical_data.empty: 
            print(f"\nTotal de velas para Backtesting: {len(historical_data)}")

            # Paso 4: Pre-procesar (Hito 3)
            processed_data = preprocess_data_for_time_bias(historical_data)

            # PASO 1: Marcar las Kill Zones con las NUEVAS CONSTANTES (14-18 UTC)
            data_with_zones = mark_kill_zones(processed_data)
            
            # PASO 2: Ejecutar el análisis de retorno
            analyze_gross_return(data_with_zones)
            
            # Paso 5: Marcar y Analizar (Hito 4)
            data_with_zones = mark_kill_zones(processed_data)
            analyze_time_bias(data_with_zones)
            analyze_all_hours(processed_data)
            
            # Una pequeña vista del etiquetado:
            print("\nVelas etiquetadas (primeras 8):")
            print(data_with_zones[['hour_utc', 'is_kill_zone', 'volume']].head(8))
        else:
            print("No se pudo obtener datos históricos o el DataFrame está vacío. Deteniendo el análisis.")
