import importlib
import os
import sys

def check_dependencies():
    dependencies = [
        'pandas', 'ccxt', 'telebot', 'ta', 'numpy', 
        'pytz', 'dateutil', 'jsonschema', 'dotenv'
    ]
    
    missing = []
    print("🔍 Iniciando validación de dependencias...")
    
    for lib in dependencies:
        try:
            importlib.import_module(lib)
            print(f"✅ {lib}: Instalado")
        except ImportError:
            missing.append(lib)
    
    # Verificación de Variables de Entorno (Secrets)
    env_vars = ['TELEGRAM_TOKEN', 'TELEGRAM_CHAT_ID', 'KRAKEN_API_KEY', 'KRAKEN_SECRET']
    missing_vars = [var for var in env_vars if not os.getenv(var)]
    
    if missing:
        print(f"❌ Faltan librerías: {', '.join(missing)}")
    if missing_vars:
        print(f"⚠️ Faltan Secrets: {', '.join(missing_vars)}")
        
    if missing or missing_vars:
        sys.exit(1)
    
    print("🚀 Entorno validado. Todo listo para ejecutar el bot.")

if __name__ == "__main__":
    check_dependencies()