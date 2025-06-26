# extract_data.py
import requests
from requests_ntlm import HttpNtlmAuth
import pandas as pd
from datetime import timedelta
from dotenv import load_dotenv
import os
import urllib3
import time
from extract_config import TAGS

# Desactiva advertencias por HTTPS sin certificado válido
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Carga las variables del archivo .env
load_dotenv()

usuario = os.getenv('PI_USERNAME')
clave = os.getenv('PI_PASSWORD')
piserver = os.getenv('PI_SERVER')
print(usuario)

# URL base para PI Web API
base_url = f'https://{piserver}/piwebapi'


# 🔍 Obtener el WebID de una variable (tag) en PI System
def obtener_webid(tag_name):
    try:
        url = f"{base_url}/points?path=\\\\{piserver}\\{tag_name}"
        response = requests.get(url, auth=HttpNtlmAuth(usuario, clave), verify=False)
        response.raise_for_status()
        return response.json()['WebId']
    except Exception as e:
        print(f"Error obteniendo WebID para {tag_name}: {e}")
        return None


# Obtener datos históricos por paginación interna
def obtener_datos_hist_pag(webid, start, end, max_per_request=25000):
    all_items = []
    current_start = start

    while True:
        url = f"{base_url}/streams/{webid}/recorded"
        params = {
            'startTime': current_start,
            'endTime': end,
            'maxCount': max_per_request,
            'boundaryType': 'Inside'
        }
        try:
            response = requests.get(
                url,
                params=params,
                auth=HttpNtlmAuth(usuario, clave),
                verify=False
            )
            response.raise_for_status()
            items = response.json().get('Items', [])
        except Exception as e:
            print(f"Error al obtener datos de {webid} entre {start} y {end}: {e}")
            items = []

        # Si no hay más datos, termina la paginación para este subrango
        if not items:
            break   

        all_items.extend(items)

        # Avanza el cursor de tiempo a partir del último valor recibido
        last_timestamp = items[-1]['Timestamp']
        last_time = pd.to_datetime(last_timestamp) + pd.Timedelta(milliseconds=1)
        current_start = last_time.isoformat()

        time.sleep(0.1)  # Evita sobrecargar el servidor

    # Convierte a DataFrame solo si hay algo útil
    return pd.DataFrame([
        {'Timestamp': i['Timestamp'], 'value': i['Value']}
        for i in all_items 
    ])


# Divide un rango largo en subrangos de N días (por defecto: 15 días)
def generar_rangos_fechas(start, end, delta_dias=15):
    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(end)
    current_start = start_date
    rangos = []

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=delta_dias), end_date)
        rangos.append((current_start.isoformat(), current_end.isoformat()))
        current_start = current_end + timedelta(milliseconds=1)

    return rangos


