# Airflow Crypto Pipeline

Pipeline de datos de criptomonedas orquestado con **Apache Airflow 3**, procesado con **PySpark + Delta Lake** y visualizado con **Streamlit**.

Extrae datos de **CoinGecko** y **Binance**, los transforma en capas Bronze → Silver → Gold y los sirve en un dashboard interactivo.

---

## Arquitectura

```
CoinGecko API  ──┐
                  ├──► Bronze (Parquet) ──► Silver (Delta Lake) ──► Gold (Delta Lake) ──► Streamlit Dashboard
Binance API    ──┘
```

| Capa | Formato | Descripción |
|------|---------|-------------|
| **Raw** | JSON (en memoria) | Respuesta directa de las APIs |
| **Bronze** | Parquet | Datos crudos con timestamp de ingesta |
| **Silver** | Delta Lake | Datos limpios, tipificados y con JOIN entre fuentes |
| **Gold** | Delta Lake | Métricas de negocio: top coins, sentimiento, volatilidad |

---

## Estructura del repositorio

```
Airflow_Crypto_Pipeline/
├── dags/
│   ├── dag_principal.py          # DAG de Airflow con las 5 tareas
│   ├── utils/
│   │   └── api_client.py         # Clientes HTTP para CoinGecko y Binance
│   ├── spark/
│   │   ├── raw_to_bronze.py      # Ingesta → Bronze (Parquet)
│   │   ├── bronze_to_silver.py   # Bronze → Silver (Delta Lake + JOIN)
│   │   └── silver_to_gold.py     # Silver → Gold (métricas de negocio)
│   ├── app/
│   │   └── dashboard.py          # Dashboard Streamlit
│   └── data/
│       └── lakehouse/            # Almacenamiento local (generado en ejecución)
├── docs/
│   └── memoria.md
├── Dockerfile.Airflow            # Imagen Airflow con JDK + dependencias Python
├── Dockerfile.Spark              # Imagen Python 3.12 + JDK + PySpark
├── docker-compose.yaml           # Orquestación de todos los servicios
├── requirements_airflow.txt
├── requirements_spark.txt
└── start.sh                      # Script para ejecutar las fases Spark manualmente
```

---

## Requisitos previos

- [Docker](https://docs.docker.com/get-docker/) ≥ 24
- [Docker Compose](https://docs.docker.com/compose/) ≥ 2.20
- Git
- ~4 GB de RAM libre para los contenedores (Spark necesita memoria)

---

## Instalación y ejecución

### 1. Clonar el repositorio

```bash
git clone https://github.com/gabrielCSSM/Airflow_Crypto_Pipeline.git
cd Airflow_Crypto_Pipeline
```

### 2. Crear el fichero de variables de entorno

El `docker-compose.yaml` necesita la variable `AIRFLOW_KEY` para la clave secreta de Airflow. Crea un fichero `.env` en la raíz del proyecto:

```bash
echo "AIRFLOW_KEY=una-clave-secreta-larga-y-aleatoria" > .env
```

### 3. Arrancar todos los servicios

```bash
docker compose up --build
```

La primera vez descargará y compilará las imágenes (~5–10 minutos dependiendo de la conexión). Los servicios que se levantan son:

| Servicio | URL | Descripción |
|----------|-----|-------------|
| `airflow-api-server` | http://localhost:8080 | UI y API de Airflow |
| `streamlit` | http://localhost:8501 | Dashboard de criptomonedas |
| `postgres` | localhost:5432 | Base de datos de metadatos de Airflow |
| `spark` | http://localhost:4040 | Spark UI (disponible durante jobs) |

### 4. Acceder a la UI de Airflow

Abre http://localhost:8080 en el navegador.

Como en el `docker-compose.yaml` está configurado `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: "true"`, **cualquier usuario/contraseña** es válido para entrar (modo admin universal, solo para desarrollo local).

### 5. Ejecutar el pipeline

Desde la UI de Airflow:

1. Localiza el DAG **`crypto_pipeline_dag`**
2. Actívalo con el toggle de la izquierda (si aparece pausado)
3. Pulsa el botón ▶ **Trigger DAG** para lanzarlo manualmente

El pipeline ejecuta estas tareas en secuencia:

```
raw_to_bronze → bronze_to_silver → silver_to_gold → validate_data → notify_done
```

### 6. Ver el dashboard

Una vez que el pipeline haya completado al menos una ejecución, abre http://localhost:8501 para ver el dashboard de Streamlit con las métricas de mercado.

---

## Parar los servicios

```bash
docker compose down
```

Para eliminar también los volúmenes (borra datos del lakehouse y logs):

```bash
docker compose down -v
```

---

## Variables de entorno relevantes

| Variable | Valor por defecto | Descripción |
|----------|-------------------|-------------|
| `AIRFLOW_KEY` | *(requerida)* | Clave secreta de Airflow (`AIRFLOW__CORE__SECRET_KEY`) |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | Evita cargar los DAGs de ejemplo |
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Ejecutor local (sin Celery ni Kubernetes) |

---

## Posibles problemas y soluciones

**El contenedor `airflow-init` falla al conectarse a Postgres**
> Espera unos segundos y repite `docker compose up`. El healthcheck de Postgres a veces tarda en estabilizarse en la primera arrancada.

**Error `delta-spark` o `pyspark` en el contenedor Airflow**
> Asegúrate de que el `Dockerfile.Airflow` instala `default-jdk` antes de instalar los paquetes Python. Java es obligatorio para PySpark.

**El dashboard muestra "tabla vacía" o no carga**
> El pipeline debe haberse ejecutado al menos una vez con éxito. Comprueba los logs del DAG en Airflow antes de abrir el dashboard.

**Puerto 8080 ya en uso**
> Cambia el mapeo en `docker-compose.yaml`: `"8888:8080"` y accede desde http://localhost:8888.

---

## Dependencias principales

| Librería | Versión | Uso |
|----------|---------|-----|
| `apache-airflow` | 3.1.8 | Orquestación del pipeline |
| `pyspark` | 4.1.1 | Procesamiento distribuido |
| `delta-spark` | 3.0.0 | Formato Delta Lake |
| `streamlit` | latest | Dashboard interactivo |
| `plotly` | 5.18.0 | Gráficas del dashboard |
| `requests` | 2.32.5 | Llamadas a las APIs |

---