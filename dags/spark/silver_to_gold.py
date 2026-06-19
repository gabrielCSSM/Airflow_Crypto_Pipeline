# Hacer que los datos tengan coherencia
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

BASE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'lakehouse')


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("CryptoPipeline-Gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# Obtenemos las 20 criptos con mas capitalización
def gold_top_coins(spark):

    print("Obteniendo las monedas con mas valor...")
    
    silver_path = os.path.join(BASE_PATH, "silver", "crypto_unified")
    df = spark.read.format("delta").load(silver_path)
    
    # Window function para rankear por market cap
    window_rank = Window.orderBy(F.col("market_cap_usd").desc())
    
    df_top = df \
        .filter(F.col("market_cap_usd").isNotNull()) \
        .withColumn("rank", F.rank().over(window_rank)) \
        .filter(F.col("rank") <= 20) \
        .select(
            "rank", "coin_id", "symbol", "name",
            "price_usd", "market_cap_usd", "volume_24h_usd",
            "price_change_1h_pct", "price_change_24h_pct", "price_change_7d_pct",
            "binance_volume_24h", "num_trades_24h", "all_time_high_usd"
        )
    
    gold_path = os.path.join(BASE_PATH, "gold", "top_coins")
    df_top.write.format("delta").mode("overwrite").save(gold_path)
    print(f"Gold top_coins: {df_top.count()} registros")


def gold_market_sentiment(spark):
    """
    Calcula el sentimiento general del mercado:
    - % de coins con precio subiendo vs bajando
    - Volumen total del mercado
    - Coin más volátil del día
    """
    print("Calculando volatividad del mercado...")
    
    silver_path = os.path.join(BASE_PATH, "silver", "crypto_unified")
    df = spark.read.format("delta").load(silver_path)
    
    df_sentiment = df.agg(
        F.count("coin_id").alias("total_coins_tracked"),
        F.sum("market_cap_usd").alias("total_market_cap_usd"),
        F.sum("volume_24h_usd").alias("total_volume_24h_usd"),
        F.avg("price_change_24h_pct").alias("avg_change_24h_pct"),
        
        # % de monedas que subieron en 24h
        (F.sum(F.when(F.col("price_change_24h_pct") > 0, 1).otherwise(0)) / F.count("coin_id") * 100)
            .alias("pct_coins_up_24h"),
    
        F.max("price_change_24h_pct").alias("best_performer_24h_pct"),
        F.min("price_change_24h_pct").alias("worst_performer_24h_pct")
    )
    
    df_sentiment = df_sentiment.withColumn("snapshot_timestamp", F.current_timestamp())
    
    gold_path = os.path.join(BASE_PATH, "gold", "market_sentiment")
    df_sentiment.write.format("delta").mode("overwrite").save(gold_path)
    print("Gold market_sentiment guardado")

# Ranking de criptomonedas por volatilidad (rango entre high y low en 24h).
def gold_volatility_ranking(spark):
    
    print("Calculando porcentaje de volatilidad...")
    
    silver_path = os.path.join(BASE_PATH, "silver", "crypto_unified")
    df = spark.read.format("delta").load(silver_path)
    
    df_vol = df \
        .filter(F.col("high_24h").isNotNull() & F.col("low_24h").isNotNull()) \
        .withColumn(
            "volatility_pct",
            F.round(((F.col("high_24h") - F.col("low_24h")) / F.col("low_24h")) * 100, 2)
        ) \
        .select("symbol", "name", "price_usd", "volatility_pct",
                "high_24h", "low_24h", "num_trades_24h") \
        .orderBy(F.col("volatility_pct").desc())
    
    gold_path = os.path.join(BASE_PATH, "gold", "volatility_ranking")
    df_vol.write.format("delta").mode("overwrite").save(gold_path)
    print(f"Gold volatility_ranking: {df_vol.count()} registros")


if __name__ == "__main__":
    spark = create_spark_session()
    gold_top_coins(spark)
    gold_market_sentiment(spark)
    gold_volatility_ranking(spark)
    spark.stop()
    print("Fase Gold completada.")