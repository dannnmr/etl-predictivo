import os
# variables que se usan para la extraccion de datos de los transformadores

TAGS = {
    "temperatura_aceite": "TR1.Top oil temperature",
    "humedad_papel": "TR1.Moisture of insulation paper",
    "temperatura_ambiente": "TR1.Ambient temperature",
    "temperatura_punto_caliente": "TR1.Hot spot temperature",
    "temperatura_burbujeo": "TR1.Bubbling temperature",
    "temperatura_aceite": "TR1.Top oil temperature",
    "tap_position": "TR1.Tap position",
    "potencia_aparente": "TR1.Power (apparent power) 1m",
    "corriente_carga": "TR1.Load current LV Ph 2",
    "voltaje": "TR1.Voltage (phase - ground) HV Ph 2",
}

START_TIME = "2024-09-10T00:00:00Z"
END_TIME = "2025-07-10T00:00:00Z"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BRONZE_DIR = os.path.join(BASE_DIR, "..", "..", "data", "bronze")
BRONZE_DIR = os.path.abspath(BRONZE_DIR)

