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

LIMIT_INTERPOLACION = 3  # Límite de interpolación para variables continuas


LIMITES = {
    "voltaje": {"min": 114, "max": 140},
    "temperatura_aceite": {"min": 0},
    "temperatura_ambiente": {"min": 0},
    "temperatura_punto_caliente": {"min":0, "max": 105},
    "temperatura_burbujeo": {"min": 0},
    "potencia_aparente": {"min": 0},
    "tap_position": {"min": 0, "max": 17},
}  


VOLTAJE_NOMINAL={
    1:242000,
    2:239250,
    3:236500,
    4:233750,
    5:231000,
    6:228250,
    7:225500,
    8:222750,
    9:220000,
    10:217250,
    11:214500,
    12:211750,
    13:209000,
    14:206250,
    15:203500,
    16:200750,
    17:198000,
} 
220438.37474716618
DESVIACION_VOLTAJE = 0.05  # 5% de desviación aceptable para voltaje




