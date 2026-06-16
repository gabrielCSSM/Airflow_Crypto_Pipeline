#!/bin/bash
python dags/spark/raw_to_bronze.py
python dags/spark/bronze_to_silver.py
python dags/spark/silver_to_gold.py
exec streamlit run dashboard.py --server.address=0.0.0.0