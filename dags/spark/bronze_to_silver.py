# Tratamiento de datos
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, TimestampType
from delta import configure_spark_with_delta_pip

BASE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'lakehouse')

def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("CryptoPipeline-Silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

# Lee Bronze de CoinGecko, limpia y tipifica los datos, y los guarda en Silver (Delta).
def transform_coingecko_to_silver(spark):
    
    print("Transformando CoinGecko Markets a Silver...")
    
    bronze_path = os.path.join(BASE_PATH, "bronze", "coingecko_markets")
    
    df = spark.read.parquet(bronze_path)
    
    # 1. Seleccionamos las columnas importantes
    df_clean = df.select(
        F.col("id").cast(StringType()).alias("coin_id"),
        F.col("symbol").cast(StringType()).alias("symbol"),
        F.col("name").cast(StringType()).alias("name"),
        F.col("current_price").cast(DoubleType()).alias("price_usd"),
        F.col("market_cap").cast(LongType()).alias("market_cap_usd"),
        F.col("total_volume").cast(LongType()).alias("volume_24h_usd"),
        F.col("price_change_percentage_24h").cast(DoubleType()).alias("price_change_24h_pct"),
        F.col("price_change_percentage_1h_in_currency").cast(DoubleType()).alias("price_change_1h_pct"),
        F.col("price_change_percentage_7d_in_currency").cast(DoubleType()).alias("price_change_7d_pct"),
        F.col("circulating_supply").cast(DoubleType()).alias("circulating_supply"),
        F.col("ath").cast(DoubleType()).alias("all_time_high_usd"),
        F.col("ingestion_timestamp").cast(TimestampType()).alias("ingestion_timestamp")
    )
    
    # 2. Limpiamos nulos
    df_clean = df_clean.filter(F.col("price_usd").isNotNull())
    df_clean = df_clean.filter(F.col("price_usd") > 0)
    
    # 3. Sustituimos nulos
    df_clean = df_clean.fillna({
        "price_change_24h_pct": 0.0,
        "price_change_1h_pct": 0.0,
        "price_change_7d_pct": 0.0
    })
    
    # 4. Estandarizamos el símbolo a mayús
    df_clean = df_clean.withColumn("symbol", F.upper(F.col("symbol")))
    
    # 5. Añadimos columna de fecha para el particionado posterior
    df_clean = df_clean.withColumn("date", F.to_date(F.col("ingestion_timestamp")))
    
    # 6. Guardamos en Delta Lake
    silver_path = os.path.join(BASE_PATH, "silver", "crypto_prices")
    
    df_clean.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("date") \
        .save(silver_path)
    
    print(f"Silver crypto_prices guardado: {df_clean.count()} registros")


# Lee Bronze de Binance, limpia y tipifica los datos, y los guarda en Silver (Delta).
def transform_binance_to_silver(spark):
    
    print("Transformando Binance Ticker a Silver...")
    
    bronze_path = os.path.join(BASE_PATH, "bronze", "binance_ticker")
    df = spark.read.parquet(bronze_path)
    
    # Limpiamos "USDT" del símbolo para poder hacer JOIN
    df_clean = df.select(
        F.regexp_replace(F.col("symbol"), "USDT$", "").alias("symbol"),
        F.col("lastPrice").cast(DoubleType()).alias("binance_price_usd"),
        F.col("volume").cast(DoubleType()).alias("binance_volume_24h"),
        F.col("priceChangePercent").cast(DoubleType()).alias("binance_change_24h_pct"),
        F.col("highPrice").cast(DoubleType()).alias("high_24h"),
        F.col("lowPrice").cast(DoubleType()).alias("low_24h"),
        F.col("count").cast(LongType()).alias("num_trades_24h"),
        F.col("ingestion_timestamp").cast(TimestampType()).alias("ingestion_timestamp")
    )
    
   
    df_clean = df_clean.withColumn("symbol", F.upper(F.col("symbol")))
    
    df_clean = df_clean.filter(F.col("binance_price_usd").isNotNull())
    df_clean = df_clean.filter(F.col("binance_price_usd") > 0)
    
    df_clean = df_clean.withColumn("date", F.to_date(F.col("ingestion_timestamp")))
    
    silver_path = os.path.join(BASE_PATH, "silver", "binance_tickers")
    
    df_clean.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("date") \
        .save(silver_path)
    
    print(f"Silver binance_tickers guardado: {df_clean.count()} registros")

# Se unen los datos de CoinGecko y Binance por símbolo.
def join_sources_to_silver(spark):
    
    print("Haciendo JOIN entre los datos en Silver...")
    
    cg_path = os.path.join(BASE_PATH, "silver", "crypto_prices")
    bn_path = os.path.join(BASE_PATH, "silver", "binance_tickers")
    
    df_cg = spark.read.format("delta").load(cg_path)
    df_bn = spark.read.format("delta").load(bn_path)
    
    # Se toma el última fecha de cada fuente para el JOIN
    max_date_cg = df_cg.agg(F.max("date")).collect()[0][0]
    max_date_bn = df_bn.agg(F.max("date")).collect()[0][0]
    
    df_cg_latest = df_cg.filter(F.col("date") == max_date_cg)
    df_bn_latest = df_bn.filter(F.col("date") == max_date_bn)
    
    # JOIN por símbolo 
    df_joined = df_cg_latest.join(
        df_bn_latest.select("symbol", "binance_price_usd", "binance_volume_24h",
                            "binance_change_24h_pct", "high_24h", "low_24h", "num_trades_24h"),
        on="symbol",
        how="left"
    )
    
    # Calculamos diferencia de precio entre las dos fuentes
    df_joined = df_joined.withColumn(
        "price_diff_pct",
        F.round(
            ((F.col("price_usd") - F.col("binance_price_usd")) / F.col("price_usd")) * 100,
            4
        )
    )
    
    joined_path = os.path.join(BASE_PATH, "silver", "crypto_unified")
    
    df_joined.write \
        .format("delta") \
        .mode("overwrite") \
        .save(joined_path)
    
    print(f"Silver crypto_unified guardado: {df_joined.count()} registros con JOIN completo")


if __name__ == "__main__":
    spark = create_spark_session()
    transform_coingecko_to_silver(spark)
    transform_binance_to_silver(spark)
    join_sources_to_silver(spark)
    spark.stop()
    print("Fase Silver completada.")