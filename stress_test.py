import pandas as pd
import numpy as np

# Re-definimos la función aquí para el test
def estratega_no_supervisado(df):
    kz_data = df[df['is_kill_zone'] == True]
    if len(kz_data) < 2: return "NEUTRAL"
    
    cuerpo_promedio = (kz_data['close'] - kz_data['open']).abs().mean()
    rango_promedio = (kz_data['high'] - kz_data['low']).mean()
    coherencia = cuerpo_promedio / rango_promedio if rango_promedio > 0 else 0
    
    if coherencia > 0.6: return "TENDENCIA_SOLIDA"
    if coherencia < 0.3: return "RUIDO_LATERAL"
    return "NEUTRAL"

print("🔬 INICIANDO SIMULACIÓN DE MERCADO...")

# --- ESCENARIO 1: RUIDO LATERAL (Mucho latigazo, poco cuerpo) ---
# Velas con mechas largas pero precio de cierre casi igual al de apertura
data_ruido = {
    'open':  [100, 101, 100, 102],
    'high':  [105, 106, 105, 107], # Mechas altas
    'low':   [95, 94, 95, 93],     # Mechas bajas
    'close': [100.5, 100.8, 100.2, 101.5], # El precio no avanza
    'is_kill_zone': [True, True, True, True]
}
df_ruido = pd.DataFrame(data_ruido)
resultado_1 = estratega_no_supervisado(df_ruido)

# --- ESCENARIO 2: TENDENCIA LIMPIA (Cuerpos largos, pocas mechas) ---
data_tendencia = {
    'open':  [100, 110, 120, 130],
    'high':  [111, 121, 131, 141],
    'low':   [99, 109, 119, 129],
    'close': [109, 119, 129, 140], # El precio avanza con fuerza
    'is_kill_zone': [True, True, True, True]
}
df_tendencia = pd.DataFrame(data_tendencia)
resultado_2 = estratega_no_supervisado(df_tendencia)

print(f"\n🚩 Resultado Mercado Ruido: {resultado_1}")
print(f"✅ Resultado Mercado Tendencia: {resultado_2}")

if resultado_1 == "RUIDO_LATERAL" and resultado_2 == "TENDENCIA_SOLIDA":
    print("\n🔥 TEST PASADO: El estratega detecta la basura y la separa del oro.")
else:
    print("\n❌ TEST FALLIDO: Hay que ajustar los umbrales de coherencia.")