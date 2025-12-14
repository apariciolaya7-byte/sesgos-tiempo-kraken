import ccxt
import os
from dotenv import load_dotenv

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