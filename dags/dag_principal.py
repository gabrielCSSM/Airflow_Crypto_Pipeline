# Dag principal
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import subprocess
import os

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
SPARK_DIR = os.path.join(DAGS_DIR, "spark")

# Faze de Bronze
def run_bronze():
    
    script = os.path.join(SPARK_DIR, "raw_to_bronze.py")
    result = subprocess.run(
        ["python", script],
        capture_output=True, text=True, check=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Fase Bronze falló:\n{result.stderr}")


# Fase Silver
def run_silver():
    script = os.path.join(SPARK_DIR, "bronze_to_silver.py")
    result = subprocess.run(
        ["python", script],
        capture_output=True, text=True, check=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Fase Silver falló:\n{result.stderr}")

# Fase Gold
def run_gold():
    script = os.path.join(SPARK_DIR, "silver_to_gold.py")
    result = subprocess.run(
        ["python", script],
        capture_output=True, text=True, check=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Fase Gold falló:\n{result.stderr}")

# Validar que la fase Gold, se compoleto y hay registros
def validate():

    BASE_PATH = os.path.join(DAGS_DIR, "data", "lakehouse")
    
    builder = (
        SparkSession.builder
        .appName("Validation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    gold_path = os.path.join(BASE_PATH, "gold", "top_coins")
    count = spark.read.format("delta").load(gold_path).count()
    
    spark.stop()
    
    if count == 0:
        raise ValueError("La tabla Gold top_coins está vacía. Algo falló en el pipeline.")
    
    print(f"✅ Validación OK: {count} registros en Gold top_coins")

with DAG(
    dag_id="crypto_pipeline_dag",
    description="Pipeline de datos de criptomonedas: CoinGecko + Binance → Delta Lake → Streamlit",
    start_date=datetime(2026, 6, 16),
    catchup=False,                   
    tags=["crypto", "lakehouse", "spark"],
    max_active_runs=1
) as dag:

    ph_bronze = PythonOperator(
        task_id="raw_to_bronze",
        python_callable=run_bronze,
        doc_md="""
        ## Fase Bronze
        Descarga datos crudos de CoinGecko y Binance API.
        Guarda los datos en Parquet sin transformaciones.
        """
    )

    ph_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_silver,
        doc_md="""
        ## Fase Silver
        Limpia y tipifica los datos de Bronze.
        Realiza JOIN entre CoinGecko y Binance.
        Guarda en Delta Lake.
        """
    )

    ph_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_gold,
        doc_md="""
        ## Fase Gold
        Calcula métricas de negocio: top coins, sentimiento, volatilidad.
        Las tablas Gold alimentan el dashboard de Streamlit.
        """
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate,
        doc_md="""
        ## Validacion de los datos
        Verifica que las tablas Gold tienen datos.
        Falla el DAG si los datos están vacíos.
        """
    )
    
    notify = BashOperator(
        task_id="notify_done",
        bash_command="echo 'Pipeline completado. Dashboard listo en http://localhost:8501'"
    )
    
    ph_bronze >> ph_silver >> ph_gold >> validate >> notify