import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

# ─── Configuración de la página ───────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard Pipeline Cryptomonedas",
    page_icon="🪙",
    layout="wide"
)

BASE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'lakehouse')


@st.cache_resource  # cachea la sesión de Spark para no recrearla cada vez
def get_spark():
    builder = (
        SparkSession.builder
        .appName("CryptoDashboard")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


@st.cache_data(ttl=300)  # refresca los datos cada 5 minutos
def load_top_coins():
    spark = get_spark()
    path = os.path.join(BASE_PATH, "gold", "top_coins")
    return spark.read.format("delta").load(path).toPandas()


@st.cache_data(ttl=300)
def load_market_sentiment():
    spark = get_spark()
    path = os.path.join(BASE_PATH, "gold", "market_sentiment")
    df = spark.read.format("delta").load(path)
    # Tomamos solo el registro más reciente
    return df.orderBy("snapshot_timestamp", ascending=False).limit(1).toPandas()


@st.cache_data(ttl=300)
def load_volatility():
    spark = get_spark()
    path = os.path.join(BASE_PATH, "gold", "volatility_ranking")
    return spark.read.format("delta").load(path).limit(20).toPandas()



st.title("Crypto Market Dashboard")
sentiment = load_market_sentiment()

if not sentiment.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    total_mcap = sentiment["total_market_cap_usd"].iloc[0]
    col1.metric("Cap. Total Mercado", f"${total_mcap/1e12:.2f}T")
    
    total_vol = sentiment["total_volume_24h_usd"].iloc[0]
    col2.metric("Volumen 24h", f"${total_vol/1e9:.1f}B")
    
    pct_up = sentiment["pct_coins_up_24h"].iloc[0]
    col3.metric("Coins al alza", f"{pct_up:.1f}%")
    
    avg_change = sentiment["avg_change_24h_pct"].iloc[0]
    col4.metric("Cambio medio 24h", f"{avg_change:.2f}%", delta=f"{avg_change:.2f}%")

st.divider()


st.subheader("Top 20 Criptomonedas * Capitalización")
top_coins = load_top_coins()

if not top_coins.empty:
    # Formateamos columnas para mejor visualización
    display_df = top_coins[["rank", "name", "symbol", "price_usd",
                              "market_cap_usd", "price_change_24h_pct",
                              "price_change_7d_pct"]].copy()
    
    display_df.columns = ["#", "Nombre", "Símbolo", "Precio (USD)",
                           "Cap. Mercado", "Cambio 24h (%)", "Cambio 7d (%)"]
    
    # Coloreamos según si sube o baja
    st.dataframe(
        display_df.style
            .format({"Precio (USD)": "${:,.4f}", "Cap. Mercado": "${:,.0f}"})
            .applymap(lambda x: "color: green" if isinstance(x, float) and x > 0
                      else ("color: red" if isinstance(x, float) and x < 0 else ""),
                      subset=["Cambio 24h (%)", "Cambio 7d (%)"]),
        use_container_width=True
    )

st.divider()

col_left, col_right = st.columns(2)
with col_left:
    st.subheader("Distribución de Capitalización")
    if not top_coins.empty:
        fig_pie = px.pie(
            top_coins.head(10),
            values="market_cap_usd",
            names="name",
            title="Top 10 por Market Cap"
        )
        st.plotly_chart(fig_pie, use_container_width=True)

with col_right:
    st.subheader("Ranking de Volatilidad (24h)")
    volatility = load_volatility()
    if not volatility.empty:
        fig_bar = px.bar(
            volatility.head(15),
            x="symbol",
            y="volatility_pct",
            title="Volatilidad (High-Low / Low × 100)",
            color="volatility_pct",
            color_continuous_scale="Reds"
        )
        st.plotly_chart(fig_bar, use_container_width=True)

st.subheader("Cambio de Precio vs Volumen de Trading")
if not top_coins.empty:
    fig_scatter = px.scatter(
        top_coins,
        x="volume_24h_usd",
        y="price_change_24h_pct",
        size="market_cap_usd",
        color="price_change_24h_pct",
        hover_name="name",
        text="symbol",
        color_continuous_scale="RdYlGn",
        title="¿Mucho volumen implica más cambio de precio?"
    )
    fig_scatter.update_traces(textposition="top center")
    st.plotly_chart(fig_scatter, use_container_width=True)