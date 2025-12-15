import ccxt
import os
from dotenv import load_dotenv
import pandas as pd
import pytz

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


def fetch_data_to_dataframe(exchange, symbol='BTC/USD', timeframe='1h', limit=24):
    """
    Descarga los datos OHLCV del exchange y los convierte a un DataFrame de Pandas.
    
    Args:
        exchange: La instancia inicializada de ccxt.kraken.
        symbol (str): El par de trading a consultar.
        timeframe (str): Intervalo de la vela (ej: '1h' para una hora).
        limit (int): Número de velas a obtener (24 velas = 24 horas).
    """
    
    if not exchange:
        print("No se pudo obtener datos: el exchange no está inicializado.")
        return None

    try:
        # 1. Obtener la Trama Horaria con ccxt.fetch_ohlcv
        print(f"Buscando {limit} velas de {timeframe} para {symbol}...")
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        
        # 2. Definir las columnas
        headers = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        
        # 3. Conversión a DataFrame de Pandas
        df = pd.DataFrame(ohlcv, columns=headers)
        
        # Convertir el timestamp (que está en milisegundos) a datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        print(f"✅ Datos descargados exitosamente. Filas: {len(df)}")
        return df
        
    except Exception as e:
        print(f"❌ Error al obtener datos OHLCV: {e}")
        return None

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
KILL_ZONE_START = 8
KILL_ZONE_END = 12

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

if __name__ == '__main__':
    # Paso 1: Inicializar la conexión
    kraken = initialize_kraken_exchange()

    if kraken:
        # Paso 2: Descargar los datos OHLCV
        # historical_data SE DEFINE AQUÍ
        historical_data = fetch_data_to_dataframe(kraken) 
        
        # Paso 3: Verificar que la descarga fue exitosa antes de continuar
        if historical_data is not None: 
            
            # Paso 4: Pre-procesar (Hito 3)
            processed_data = preprocess_data_for_time_bias(historical_data)
            
            # Paso 5: Marcar y Analizar (Hito 4)
            data_with_zones = mark_kill_zones(processed_data)
            analyze_time_bias(data_with_zones)
            
            # Una pequeña vista del etiquetado:
            print("\nVelas etiquetadas (primeras 8):")
            print(data_with_zones[['hour_utc', 'is_kill_zone', 'volume']].head(8))
        else:
            print("No se pudo obtener datos históricos. Deteniendo el análisis.")
