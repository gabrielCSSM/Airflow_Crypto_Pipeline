# Cliente que se va utilizar para hacer peticiones a las API's seleccionadas
import requests
import json
from datetime import datetime

def get_cg_markets(vs_currency="usd", per_page=50):
    # Se obtienen las divisas mas populares + sus métricas de mercado.
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "1h,24h,7d"
    }
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def get_binance_tick_24(symbols=None):
    # Ultimas 24h de estadisticas de Binance.
    url = "https://api.binance.com/api/v3/ticker/24hr"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    
    # Filtramos solo los pares contra USDT para coherencia con CoinGecko
    if symbols is None:
        data = [item for item in data if item["symbol"].endswith("USDT")]
    
    return data


def get_cg_global():
    # Métricas globales de todo el mercado.
    url = "https://api.coingecko.com/api/v3/global"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()