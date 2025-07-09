# etl/capa_silver/config.py

import os

# Ruta base (raíz del proyecto)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Ruta a los datos Bronze
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")

# Ruta donde se guardará Silver
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
os.makedirs(SILVER_DIR, exist_ok=True)

# Frecuencia objetivo de resampleo
FRECUENCIA = "15min"  # 15 minutos

# Variables por tipo
VARIABLES_CONTINUAS = [
    "voltaje",
    "corriente_carga",
    "temperatura_aceite",
    "temperatura_ambiente",
    "temperatura_punto_caliente",
    "temperatura_burbujeo",
    "potencia_aparente",
]

# VARIABLES_CRITICAS = [
# ]

VARIABLES_DISCRETAS = [
    "tap_position"
]

TODAS_LAS_VARIABLES = VARIABLES_CONTINUAS + VARIABLES_DISCRETAS
