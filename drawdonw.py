import json
import os

def audit_drawdown():
    BANK_FILE = 'virtual_bank.json'
    INITIAL_CAPITAL = 500.0
    
    if not os.path.exists(BANK_FILE):
        print("❌ No hay datos bancarios para auditar.")
        return

    with open(BANK_FILE, 'r') as f:
        data = json.load(f)
        current_balance = data.get('balance', INITIAL_CAPITAL)

    # Cálculo de métricas
    profit_loss = current_balance - INITIAL_CAPITAL
    profit_percentage = (profit_loss / INITIAL_CAPITAL) * 100
    
    # Simulación de Drawdown (basada en el balance actual vs el pico de 500)
    # En una versión Pro, guardaríamos el 'peak_balance' en el JSON.
    drawdown = 0
    if current_balance < INITIAL_CAPITAL:
        drawdown = ((INITIAL_CAPITAL - current_balance) / INITIAL_CAPITAL) * 100

    print("📊 --- AUDITORÍA DE RIESGO ---")
    print(f"💰 Balance Actual: ${current_balance:.2f}")
    print(f"📈 PnL Neto: ${profit_loss:.2f} ({profit_percentage:.2f}%)")
    print(f"📉 Drawdown Actual: {drawdown:.2f}%")
    
    if drawdown > 5:
        print("⚠️ ALERTA: El riesgo está superando el límite del 5%. Revisar ATR.")
    else:
        print("✅ Riesgo bajo control. Gestión de capital saludable.")

# Puedes llamar a esto desde un comando de Telegram /audit