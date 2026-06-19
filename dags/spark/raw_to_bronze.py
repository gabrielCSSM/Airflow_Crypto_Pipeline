# Ingesta de datos
import json
import os
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, date_format

# Cliente
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.api_client import get_cg_markets, get_binance_tick_24, get_cg_global

# Lakehouse simulado
BASE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'lakehouse')


def create_spark_session():
    # Config para poder leer e escribir  en tablas Delta.
    return (
        SparkSession.builder
        .appName("CryptoPipeline-Bronze")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

# Metodo para crear DF de spark (Revisable para mejora)
def create_spark_df(raw_data, attachement):
    pandas_df = pd.json_normalize(raw_data)
    
    spark_df = spark.createDataFrame(pandas_df)
    
    spark_df = spark_df.withColumn("ingestion_timestamp", current_timestamp())
    spark_df = spark_df.withColumn("source", lit(f"{attachement}"))
    
    spark_df = spark_df.withColumn(
        "ingestion_date", date_format("ingestion_timestamp", "yyyy-MM-dd")
    )
    
    return spark_df
    

# Descarga los datos de mercado de CoinGecko y los guarda en Bronze.
def ingest_coingecko_markets(spark):
    
    print("Descargando datos de CoinGecko...")
    raw_data = get_cg_markets(per_page=100)
    
    df = create_spark_df(raw_data, "coingecko_markets")
    
    output_path = os.path.join(BASE_PATH, "bronze", "coingecko_markets")
    
    df.write \
    .mode("overwrite") \
    .partitionBy("source", "ingestion_date") \
    .option("partitionOverwriteMode", "dynamic") \
    .parquet(output_path)
    
    print(f"CoinGecko Markets guardado en {output_path} ({df.count()} registros)")
    return df.count()


#Descarga los tickers de 24h de Binance y los guarda en Bronze.
def ingest_binance_ticker(spark):
    
    print("Descargando datos de Binance...")
    raw_data = get_binance_tick_24()
    
    df = create_spark_df(raw_data, "binance_ticker_24h")

    output_path = os.path.join(BASE_PATH, "bronze", "binance_ticker")
    
    df.write \
    .mode("overwrite") \
    .partitionBy("source", "ingestion_date") \
    .option("partitionOverwriteMode", "dynamic") \
    .parquet(output_path)
    
    print(f"Binance Ticker guardado en {output_path} ({df.count()} registros)")
    return df.count()


# Descarga métricas globales del mercado y las guarda en Bronze.
def ingest_global_market(spark):
    
    print("Descargando métricas globales del mercado...")
    raw_data = get_cg_global()
    
    global_data = raw_data.get("data", {})
    
    # Creamos un solo registro con timestamp
    record = {
        "total_market_cap_usd": str(global_data.get("total_market_cap", {}).get("usd", 0)),
        "total_volume_usd": str(global_data.get("total_volume", {}).get("usd", 0)),
        "btc_dominance": global_data.get("market_cap_percentage", {}).get("btc", 0),
        "eth_dominance": global_data.get("market_cap_percentage", {}).get("eth", 0),
        "active_cryptocurrencies": global_data.get("active_cryptocurrencies", 0),
        "raw_json": json.dumps(raw_data)
    }
    
    df = spark.createDataFrame([record])
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("source", lit("coingecko_global"))
    df = df.withColumn("ingestion_date", date_format("ingestion_timestamp", "yyyy-MM-dd"))  # ← añadir esto
    
    output_path = os.path.join(BASE_PATH, "bronze", "global_market")
    
    df.write \
    .mode("overwrite") \
    .partitionBy("ingestion_date") \
    .option("partitionOverwriteMode", "dynamic") \
    .parquet(output_path)
      
    
    print(f"Métricas globales guardadas en {output_path}")


if __name__ == "__main__":
    spark = create_spark_session()
    
    ingest_coingecko_markets(spark)
    ingest_binance_ticker(spark)
    ingest_global_market(spark)
    
    spark.stop()
    print("Fase Bronze completada.")