import ccxt
import os
from dotenv import load_dotenv
import pandas as pd

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

if __name__ == '__main__':
    kraken = initialize_kraken_exchange()
    if kraken:
        # Probamos la nueva función
        historical_data = fetch_data_to_dataframe(kraken)
        if historical_data is not None:
            print("\nPrimeras 5 filas del DataFrame:")
            print(historical_data.head())
            print(f"\nTipos de datos:\n{historical_data.dtypes}")
