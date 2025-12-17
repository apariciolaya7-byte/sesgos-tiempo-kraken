import streamlit as st
import pandas as pd
import json
import os
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title="Dashboard Sesgos Kraken")
st.title("Dashboard de Sesgos de Tiempo - Kraken Bot")

# Estilos visuales: fondo, tipografía, transparencias y tarjetas
st.markdown(
    """
    <style>
    /* Tipografía personalizada */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; }

    /* Fondo de la app (imagen + degradado sutil) */
    div[data-testid="stAppViewContainer"] {
        background-image: linear-gradient(rgba(6,16,34,0.45), rgba(6,16,34,0.45)), url('https://images.unsplash.com/photo-1564869733183-6b9a2b3d9f39?auto=format&fit=crop&w=1600&q=60');
        background-size: cover;
        background-position: center;
        background-attachment: fixed;
    }

    /* Panel principal con fondo translúcido y blur */
    .block-container {
        background: rgba(255,255,255,0.7);
        backdrop-filter: blur(6px);
        border-radius: 12px;
        padding: 1rem 1.2rem;
        box-shadow: 0 8px 30px rgba(2,18,43,0.25);
    }

    /* Header estilizado */
    header[role="banner"] {
        background: linear-gradient(90deg, rgba(11,127,218,0.9), rgba(15,155,142,0.9));
        color: white;
        border-bottom-left-radius: 12px;
        border-bottom-right-radius: 12px;
        box-shadow: 0 6px 20px rgba(2,18,43,0.25);
    }

    /* Sidebar translúcido */
    section[data-testid="stSidebar"] .css-1d391kg, section[data-testid="stSidebar"] .css-1d391kg * {
        background: rgba(0,0,0,0.55) !important;
        color: #ffffff !important;
    }

    /* Tarjetas y layout en la columna derecha */
    .css-1v3fvcr { /* clase generada por Streamlit para contenedores */
        background: rgba(255,255,255,0.65);
        border-radius: 10px;
        padding: 0.6rem;
        box-shadow: 0 6px 18px rgba(2,18,43,0.12);
    }

    /* Títulos y texto para mejor contraste */
    h1, h2, h3, .stMetricValue, .stMetricDelta {
        color: #02122b !important;
    }

    /* Botones con estilo */
    div.stButton > button {
        background: linear-gradient(90deg,#0f9b8e,#0b7fda);
        color: white;
        border: none;
        padding: 0.45rem 0.8rem;
        border-radius: 8px;
        box-shadow: 0 6px 14px rgba(11,127,218,0.18);
    }

    /* Inputs y select con bordes suaves */
    .stTextInput, .stSelectbox, .stNumberInput {
        border-radius: 8px;
    }

    /* Logo flotante en el header */
    .app-logo {
        position: absolute;
        left: 16px;
        top: 8px;
        width: 48px;
        height: 48px;
        border-radius: 8px;
        box-shadow: 0 6px 18px rgba(2,18,43,0.2);
        background: white;
        padding: 6px;
    }

    /* DataFrame y tablas con borde sutil */
    .stDataFrame table {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 6px 18px rgba(2,18,43,0.06);
    }

    /* Métricas ligeramente más grandes */
    .stMetricValue {
        font-size: 1.2rem !important;
    }

    </style>
    """,
    unsafe_allow_html=True,
)

# Intentar importar funciones clave del bot de `kraken_data.py` de forma segura
kraken_available = True
try:
    from kraken_data import (
        initialize_kraken_exchange,
        fetch_recent_data,
        preprocess_data_for_time_bias,
        mark_kill_zones,
        analyze_gross_return,
        calculate_atr,
        execute_trade_simulation,
        load_open_positions,
        OPEN_POSITIONS,
        CLOSED_TRADES,
        save_open_positions,
        monitor_and_close_positions,
    )
except Exception as e:
    kraken_available = False


st.sidebar.header("Controles")
mode = st.sidebar.selectbox("Modo de datos", ["Local (CSV)", "Vivo (Kraken)"])

# Listar símbolos disponibles por CSV
analysis_files = [f for f in os.listdir('.') if f.endswith('_time_bias_hourly_analysis.csv')]
symbols = [f.replace('_time_bias_hourly_analysis.csv', '') for f in analysis_files]
if not symbols:
    symbols = ['BTC_USD', 'ETH_USD']

selected_symbol = st.sidebar.selectbox("Seleccionar símbolo", symbols)
timeframe = st.sidebar.text_input("Timeframe", value='1h')
hours_to_analyze = st.sidebar.slider("Horas a analizar", min_value=10, max_value=200, value=50)
atr_multiplier = st.sidebar.number_input("ATR Multiplier", value=0.05, format="%.4f")


st.header("Análisis y Visualizaciones")

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader(f"Análisis para {selected_symbol}")

    if mode == "Local (CSV)":
        file_path = f"{selected_symbol}_time_bias_hourly_analysis.csv"
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            if 'hour_utc' in df.columns:
                df['hour_utc'] = df['hour_utc'].astype(int)

            fig_vol = px.bar(df, x='hour_utc', y='avg_volume', title='Volumen Promedio por Hora UTC')
            fig_vol.update_traces(marker_color='#0b7fda', marker_line_color='rgba(255,255,255,0.12)')
            fig_vol.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#02122b'))
            st.plotly_chart(fig_vol, use_container_width=True)

            fig_range = px.bar(df, x='hour_utc', y='avg_range', title='Rango Promedio por Hora UTC')
            fig_range.update_traces(marker_color='#0f9b8e', marker_line_color='rgba(255,255,255,0.12)')
            fig_range.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#02122b'))
            st.plotly_chart(fig_range, use_container_width=True)

            st.dataframe(df)
        else:
            st.warning(f"Archivo {file_path} no encontrado. Mostrar solo resumen de CSV disponibles.")
            st.write(analysis_files)

    else:
        # Modo vivo
        if not kraken_available:
            st.error("Integración con `kraken_data.py` no disponible. Revisa dependencias.")
        else:
            # Conectar a Kraken (si se solicita)
            if st.button("Conectar a Kraken y obtener datos"):
                kraken = initialize_kraken_exchange()
                if not kraken:
                    st.error("No se pudo inicializar Kraken (revise variables de entorno).")
                else:
                    st.success("Conexión a Kraken establecida.")

                    # Obtener datos recientes
                    df_hist = fetch_recent_data(kraken, symbol=selected_symbol.replace('_', '/'), timeframe=timeframe, limit=hours_to_analyze)
                    if df_hist is None:
                        st.warning("No se obtuvieron datos OHLCV.")
                    else:
                        df_proc = preprocess_data_for_time_bias(df_hist.copy())
                        df_zones = mark_kill_zones(df_proc.copy())
                        score = analyze_gross_return(df_zones)
                        atr_val = calculate_atr(df_hist.copy())

                        st.metric("Score (Gross Return KZ)", f"{score:.4f}")
                        st.metric("ATR (última vela)", f"${atr_val:.4f}")

                        # Gráfico de precios con Kill Zone resaltada
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=df_hist['timestamp'], y=df_hist['close'], name='Close', line=dict(color='#0b7fda', width=2)))
                        # Añadir sombreado para kill zones
                        kz = df_zones[df_zones['is_kill_zone'] == True]
                        if not kz.empty:
                            for idx, row in kz.iterrows():
                                fig.add_vrect(x0=row['timestamp'], x1=row['timestamp'], fillcolor='LightSalmon', opacity=0.3, line_width=0)

                        fig.update_layout(title=f"Precio Close - {selected_symbol}", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#02122b'))
                        st.plotly_chart(fig, use_container_width=True)

                        if st.button("Simular Trade usando score y ATR"):
                            execute_trade_simulation(selected_symbol.replace('_', '/'), score, atr_multiplier, df_hist)
                            st.success("Simulación ejecutada — revisar posiciones abiertas.")

with col2:
    st.subheader("Posiciones Abiertas")
    # Intentar cargar posiciones usando la función del módulo si está disponible
    if os.path.exists('open_positions.json'):
        try:
            with open('open_positions.json', 'r') as f:
                positions = json.load(f)
        except Exception:
            positions = []
    else:
        positions = []

    if positions:
        st.json(positions)
    else:
        st.write("No hay posiciones abiertas.")

    st.subheader("Resultados de Backtesting / Trades cerrados")
    if os.path.exists('backtesting_results.csv'):
        backtest_df = pd.read_csv('backtesting_results.csv')
        st.dataframe(backtest_df)
        if 'pnl_usd' in backtest_df.columns:
            fig_pnl = px.bar(backtest_df, x=backtest_df.index, y='pnl_usd', title='PnL por Trade')
            fig_pnl.update_traces(marker_color='#ff7b5c')
            fig_pnl.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#02122b'))
            st.plotly_chart(fig_pnl)
            total_pnl = backtest_df['pnl_usd'].sum()
            st.metric("PnL Total", f"${total_pnl:.2f}")
    else:
        # Si el módulo cargó, mostrar CLOSED_TRADES en memoria
        if kraken_available:
            try:
                if CLOSED_TRADES:
                    st.dataframe(pd.DataFrame(CLOSED_TRADES))
                else:
                    st.write("No hay trades cerrados en memoria.")
            except Exception:
                st.write("No hay archivo de backtesting y no hay trades cerrados.")
        else:
            st.write("No se encontraron resultados de backtesting.")

st.markdown("---")
st.write("Notas: \n- Modo Local usa los CSV generados por el análisis horario.\n- Modo Vivo intenta conectar a Kraken y requiere variables de entorno `KRAKEN_API_KEY` y `KRAKEN_SECRET`.\n- Ejecutar `streamlit run dashboard.py` para iniciar el dashboard.")