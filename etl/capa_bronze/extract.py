# etl/bronze/extract.py
import os
import time
import pandas as pd
import requests
from requests_ntlm import HttpNtlmAuth
from dotenv import load_dotenv
import urllib3
from datetime import timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()
"""

"""

# Configuraciones desde .env
USUARIO = os.getenv('PI_USERNAME')
CLAVE = os.getenv('PI_PASSWORD')
PISERVER = os.getenv('PI_SERVER')
BASE_URL = f'https://{PISERVER}/piwebapi'

HEADERS = {'Content-Type': 'application/json'}

#funcion para obtener el WebID de un punto especifico
def obtener_webid(tag_name):
    url = f"{BASE_URL}/points?path=\\\\{PISERVER}\\{tag_name}"
    response = requests.get(url, auth=HttpNtlmAuth(USUARIO, CLAVE), verify=False)
    response.raise_for_status()
    return response.json()['WebId']


def generar_rangos_fechas(start_date_str, end_date_str, delta_dias=15):
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)
    rangos = []
    current_start = start_date
    
    print(f"current: {current_start}, end: {end_date}")

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=delta_dias), end_date)
        rangos.append((current_start.isoformat(), current_end.isoformat()))
        current_start = current_end + timedelta(seconds=1)

    return rangos


def obtener_datos_hist_pag(webid, start, end, max_per_request=800000):
    all_items = []
    current_start = start

    while True:
        url = f"{BASE_URL}/streams/{webid}/recorded"
        params = {
            'startTime': current_start,
            'endTime': end,
            'maxCount': max_per_request,
            'boundaryType': 'Inside'
        }
        response = requests.get(url, params=params, auth=HttpNtlmAuth(USUARIO, CLAVE), verify=False)
        response.raise_for_status()
        items = response.json().get('Items', [])
        if not items:
            break
        all_items.extend(items)
        last_timestamp = items[-1]['Timestamp']
        last_time = pd.to_datetime(last_timestamp) + pd.Timedelta(milliseconds=1)
        current_start = last_time.isoformat()
        time.sleep(0.1)
        if len(items) < max_per_request:
            break

    df = pd.DataFrame([{'timestamp': item['Timestamp'], 'value': item['Value']} for item in all_items])
    return df
