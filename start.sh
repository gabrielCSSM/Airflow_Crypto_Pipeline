#!/bin/bash
python dags/spark/raw_to_bronze.py
python dags/spark/bronze_to_silver.py
python dags/spark/silver_to_gold.py
