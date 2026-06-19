# Memoria Técnica: Cryptocurrency Pipeline

---

## 1. Descripción del problema

### ¿Qué se quiere analizar?

El mercado de criptomonedas es uno de los más dinámicos y volátiles del mundo financiero. Los precios cambian en cuestión de segundos y los volúmenes de trading fluctúan continuamente. La necesidad de disponer de datos actualizados, fiables y consolidados es fundamental para cualquier análisis o toma de decisión en este contexto.

Este proyecto aborda la construcción de un pipeline de datos automatizado que recoge, transforma y presenta información de mercado de las principales criptomonedas cada hora. El sistema integra dos fuentes de datos complementarias (CoinGecko y Binance), las procesa mediante una arquitectura por capas y expone los resultados a través de un dashboard interactivo.

### ¿Por qué es útil?

El pipeline construido tiene aplicaciones reales en varios contextos:

- **Seguimiento de mercado:** permite monitorizar en tiempo cuasi-real el precio, capitalización y volumen de las principales criptomonedas.
- **Detección de oportunidades:** el cálculo de volatilidad y la comparación de precios entre exchanges facilita la identificación de movimientos relevantes.
- **Validación cruzada de precios:** al combinar dos fuentes independientes, es posible detectar discrepancias de precio entre CoinGecko (datos agregados) y Binance (exchange real).
- **Análisis de sentimiento de mercado:** el porcentaje de monedas al alza, el volumen total y el cambio medio dan una visión global del estado del mercado.

---

## 2. Fuentes de datos

### 2.1 CoinGecko API

CoinGecko es una plataforma de datos de criptomonedas que agrega información de múltiples exchanges. Se utilizan dos endpoints:

**`/api/v3/coins/markets`** — Datos de mercado por moneda:

| Campo | Descripción |
|---|---|
| `id`, `symbol`, `name` | Identificador y nombre de la moneda |
| `current_price` | Precio actual en USD |
| `market_cap` | Capitalización de mercado |
| `total_volume` | Volumen de las últimas 24h |
| `price_change_percentage_1h/24h/7d` | Variaciones temporales |
| `circulating_supply` | Oferta circulante |
| `ath` | Precio máximo histórico |

**`/api/v3/global`** — Métricas globales del mercado:

Incluye capitalización total, volumen total y dominancia de Bitcoin y Ethereum.

> **Tener en cuenta** que el tier gratuito permite 60 llamadas por minuto y no requiere API key, lo que simplifica la configuración. Dado que el pipeline se ejecuta cada hora (una sola llamada por ejecución), no se alcanza este límite en ningún caso.

### 2.2 Binance API

Binance es el mayor exchange de criptomonedas por volumen. Se utiliza el endpoint:

**`/api/v3/ticker/24hr`** — Estadísticas de las últimas 24 horas para todos los pares de trading:

| Campo | Descripción |
|---|---|
| `symbol` | Par de trading (ej. BTCUSDT) |
| `lastPrice` | Último precio negociado |
| `volume` | Volumen en 24h |
| `priceChangePercent` | Variación porcentual 24h |
| `highPrice` / `lowPrice` | Máximo y mínimo del día |
| `count` | Número de operaciones realizadas |

Se filtran únicamente los pares contra USDT para mantener coherencia con la divisa de referencia de CoinGecko (USD).

### 2.3 Por qué combinar ambas fuentes

Las dos fuentes se complementan de forma natural:

- **CoinGecko** proporciona datos agregados (múltiples exchanges) junto con métricas adicionales como capitalización, oferta circulante y precio histórico máximo. Es la fuente de referencia para datos fundamentales.
- **Binance** proporciona datos de operaciones reales en un exchange concreto: número de trades, máximos y mínimos intradía y volumen operado. Es especialmente útil para calcular la volatilidad real del mercado.

La diferencia de precio entre ambas fuentes (`price_diff_pct`) permite detectar posibles situaciones de arbitraje o anomalías en los datos.

---

## 3. Arquitectura del pipeline

### Diagrama de la arquitectura

```
┌─────────────────────┐    ┌─────────────────────┐
│   CoinGecko API     │    │    Binance API       │
│  /coins/markets     │    │  /ticker/24hr        │
│  /global            │    │  (solo pares USDT)   │
└────────┬────────────┘    └──────────┬───────────┘
         │                            │
         └────────────┬───────────────┘
                      │  Airflow (cada hora)
                      ▼
         ┌────────────────────────┐
         │    CAPA BRONZE         │
         │  Datos crudos en       │
         │  formato Parquet       │
         │  (sin transformar)     │
         └────────────┬───────────┘
                      │  PySpark
                      ▼
         ┌────────────────────────┐
         │    CAPA SILVER         │
         │  Datos limpios y       │
         │  tipados en Delta Lake │
         │  JOIN CoinGecko+Binance│
         └────────────┬───────────┘
                      │  PySpark + Window Functions
                      ▼
         ┌────────────────────────┐
         │    CAPA GOLD           │
         │  Métricas agregadas    │
         │  en Delta Lake         │
         │  (top coins,           │
         │   sentimiento,         │
         │   volatilidad)         │
         └────────────┬───────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  Dashboard Streamlit   │
         │  (visualización        │
         │   interactiva)         │
         └────────────────────────┘
```

### Descripción de capas

| Capa | Propósito | Formato | Modo de escritura |
|---|---|---|---|
| **Bronze** | Almacenamiento fiel de los datos crudos de la API | Parquet | `append` |
| **Silver** | Limpieza, tipado y unificación de fuentes | Delta Lake | `append` (por fuente) / `overwrite` (unificado) |
| **Gold** | Métricas pre-calculadas para el dashboard | Delta Lake | `overwrite` (top/volatilidad) / `append` (sentimiento) |
| **Consumo** | Visualización interactiva de los datos Gold | Streamlit | — |

---

## 4. Explicación de las capas

### 4.1 Capa Bronze — Ingestión de datos crudos

La capa Bronze recibe los datos directamente de las APIs sin aplicar ninguna transformación. Este principio es fundamental en la arquitectura Medallion: los datos originales siempre deben estar preservados para poder reprocesarlos en caso de error o cambio de lógica de negocio.

Se ingieren tres conjuntos de datos:

- `cg_markets` — Top 100 monedas por capitalización
- `binance_tick_24` — Todos los pares USDT de Binance
- `cg_global` — Métricas globales del mercado

A cada registro se le añaden dos columnas de auditoría: `ingestion_timestamp` (marca de tiempo de la ingestión) y `source` (identificador de la fuente). Esto permite trazabilidad completa de cada dato.

### 4.2 Capa Silver — Limpieza y transformación

La capa Silver aplica las siguientes transformaciones sobre los datos de Bronze:

**Selección de columnas relevantes:** se descartan los campos que no aportan valor al análisis (URLs de imágenes, identificadores internos, etc.) y se renombran las columnas a nombres semánticos claros.

**Tipado explícito:** todas las columnas numéricas se castean a sus tipos correctos (`DoubleType`, `LongType`, `StringType`, `TimestampType`). Las APIs pueden devolver valores numéricos como strings en ciertos contextos, y el tipado explícito garantiza la consistencia.

**Eliminación de datos corruptos:** se filtran las filas con precio nulo o igual a cero, que corresponden a datos incorrectos o monedas sin mercado activo.

**Imputación de nulos:** los porcentajes de variación nulos se rellenan con 0.0, ya que la ausencia de dato equivale en este contexto a ausencia de movimiento.

**Estandarización de símbolos:** todos los símbolos se convierten a mayúsculas para garantizar la coherencia en el JOIN posterior. Además, en los datos de Binance se elimina el sufijo "USDT" del símbolo (ej. "BTCUSDT" → "BTC") para que la clave de unión sea equivalente en ambas fuentes.

**Particionado por fecha:** los datos se particionan por la columna `date` derivada del `ingestion_timestamp`. Esto mejora el rendimiento de las consultas cuando se filtran por fecha.

**JOIN entre fuentes:** el elemento más relevante de Silver es la unión de los datos de CoinGecko y Binance. Se realiza un `left join` por la columna `symbol`, tomando como base los datos de CoinGecko (fuente principal) y enriqueciendo con los datos de Binance donde estén disponibles. El resultado se almacena en `silver/crypto_unified` y además incluye la columna `price_diff_pct`, que cuantifica la diferencia porcentual de precio entre las dos fuentes.

### 4.3 Capa Gold — Agregaciones y métricas de negocio

La capa Gold contiene tres tablas optimizadas para su consumo directo en el dashboard:

**`gold/top_coins`** — Top 20 criptomonedas por capitalización de mercado. Se utiliza una window function `rank()` ordenada por `market_cap_usd` descendente para asignar la posición a cada moneda sin reducir las filas del DataFrame. Esta tabla es la principal del dashboard.

**`gold/market_sentiment`** — Una única fila con métricas globales del mercado: capitalización total, volumen total, cambio medio en 24h, porcentaje de monedas al alza, mejor y peor rendimiento del día. Esta tabla se escribe en modo `append` para mantener histórico de sentimiento.

**`gold/volatility_ranking`** — Ranking de monedas ordenadas por volatilidad intradía, calculada como `(high_24h - low_24h) / low_24h × 100`. Útil para identificar las monedas con mayor movimiento de precio en el día.

---

## 5. Descripción del DAG de Airflow

### Configuración del DAG

```
    dag_id="crypto_pipeline_dag",
    description="Pipeline de datos de criptomonedas: CoinGecko + Binance → Delta Lake → Streamlit",
    start_date=datetime(2026, 6, 16),
    schedule="@daily",
    catchup=False,                   
    tags=["crypto", "lakehouse", "spark"],
    max_active_runs=1
```

### Tareas y dependencias

El DAG define cinco tareas que se ejecutan de forma secuencial:

```
ph_bronze >> ph_silver >> ph_gold >> validate >> notify
```

| Tarea | Función | Script |
|---|---|---|
| `ph_bronze` | Descarga datos de CoinGecko y Binance y los guarda en Parquet | `spark/raw_to_bronze.py` |
| `ph_silver` | Limpia, tipifica y hace el JOIN entre fuentes en Delta Lake | `spark/bronze_to_silver.py` |
| `ph_gold` | Calcula las métricas del dashboard y las guarda en Gold | `spark/silver_to_gold.py` |
| `validate` | Verifica que la tabla `gold/top_coins` tiene registros | - |
| `notify` | Da por consola la direccion de donde esta alojado streamlit que la tabla `gold/top_coins` tiene registros | - |

La tarea de validación actúa como un control de calidad: consulta la tabla `gold/top_coins` y lanza una excepción si está vacía, lo que garantiza que el pipeline no se da por completado si los datos no se han procesado correctamente.

---

## 6. Transformaciones principales

### 6.1 Limpieza de nulos y tipado de columnas

La API de CoinGecko puede devolver campos numéricos como `null` cuando una moneda no tiene datos de mercado suficientes (por ejemplo, `price_change_percentage_7d` puede ser nulo para monedas recién listadas). El proceso de Silver aplica:

```python
df_clean = df_clean.filter(F.col("price_usd").isNotNull())  # elimina filas sin precio
df_clean = df_clean.filter(F.col("price_usd") > 0)          # elimina precios inválidos
df_clean = df_clean.fillna({                                   # imputa nulos en variaciones
    "price_change_24h_pct": 0.0,
    "price_change_1h_pct": 0.0,
    "price_change_7d_pct": 0.0
})
```

El tipado explícito con `.cast()` convierte todos los campos a su tipo correcto, garantizando que las operaciones aritméticas posteriores (sumas, medias, rankings) funcionen sin errores de tipo.

### 6.2 Estandarización de símbolos

Para que el JOIN entre CoinGecko y Binance funcione correctamente, los símbolos deben ser idénticos en ambas fuentes:

- **CoinGecko** devuelve símbolos en minúsculas: `btc`, `eth`, `sol`
- **Binance** devuelve pares en mayúsculas con sufijo: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`

Las transformaciones aplicadas son:

```python
# En CoinGecko:
df_clean = df_clean.withColumn("symbol", F.upper(F.col("symbol")))
# btc → BTC

# En Binance:
F.regexp_replace(F.col("symbol"), "USDT$", "").alias("symbol")
# BTCUSDT → BTC
```

Con ambas transformaciones, la columna `symbol` queda como `BTC`, `ETH`, etc. en las dos fuentes, permitiendo el JOIN.

### 6.3 El JOIN entre CoinGecko y Binance

```python
df_joined = df_cg_latest.join(
    df_bn_latest.select("symbol", "binance_price_usd", "binance_volume_24h",
                        "binance_change_24h_pct", "high_24h", "low_24h", "num_trades_24h"),
    on="symbol",
    how="left"
)
```

El resultado enriquece cada moneda de CoinGecko con los datos de Binance correspondientes. La columna calculada `price_diff_pct` refleja la diferencia porcentual de precio entre las dos fuentes:

```python
df_joined = df_joined.withColumn(
    "price_diff_pct",
    F.round(
        ((F.col("price_usd") - F.col("binance_price_usd")) / F.col("price_usd")) * 100,
        4
    )
)
```

### 6.4 Window Functions para el ranking

La capa Gold utiliza window functions de Spark para calcular el ranking de monedas sin necesidad de reducir el número de filas (a diferencia de `groupBy`, que agrega):

```python
window_rank = Window.orderBy(F.col("market_cap_usd").desc())

df_top = df \
    .filter(F.col("market_cap_usd").isNotNull()) \
    .withColumn("rank", F.rank().over(window_rank)) \
    .filter(F.col("rank") <= 20)
```

La función `F.rank()` asigna la posición 1 a la moneda con mayor capitalización, 2 a la segunda, y así sucesivamente. El filtro posterior selecciona únicamente el top 20.

---

## 7. Decisiones técnicas

### `mode("append")` en Bronze vs `mode("overwrite")` en Gold

- **Bronze usa `append`** porque cada ejecución del pipeline representa un snapshot diferente en el tiempo. Acumular estos snapshots permite análisis histórico de la evolución de precios.
- **Gold usa `overwrite`** en las tablas de ranking y volatilidad porque estas tablas representan el estado *actual* del mercado, calculado siempre desde Silver con los datos más recientes. Sobrescribir garantiza que el dashboard siempre muestra datos frescos sin acumulación innecesaria.

### `left join` en lugar de `inner join`

El `inner join` descartaría todas las monedas de CoinGecko que no tengan par USDT en Binance: monedas de baja capitalización, tokens de ecosistemas específicos (Solana, Cardano) o criptomonedas que solo se operan en otros exchanges. Esto reduciría artificialmente el universo de análisis. Con el `left join`, todas las monedas de CoinGecko están presentes en la tabla unificada, y simplemente los campos de Binance quedan como `null` cuando no hay correspondencia.

---

## 8. Dificultades encontradas

### Airflow y sus dependencias

Debido a como esta hecho airflow y como interactua con las depencias que este puede llegar a necesitar, y sumando el hecho de que he usado una imagen custom para hacer este proyecto, muchos de los problemas y errore de compatibilidad vineron por ese motivo. Menos mal, que Airflow proporciona una lista de depencias validas por version.

> **Esta es la que yo utilice**
https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt

### Rate limiting de las APIs

La API gratuita de CoinGecko tiene un límite de 60 llamadas por minuto. Si el pipeline se ejecuta con frecuencia o se realizan pruebas manuales repetidas, pueden producirse errores HTTP 429 (Too Many Requests). La solución es utilizar `.raise_for_status()` para detectar el error y dejar que el mecanismo de reintentos de Airflow (`retries=2` con `retry_delay=timedelta(minutes=5)`) gestione la recuperación automática.


---