import requests
from requests_ntlm import HttpNtlmAuth
import pandas as pd
import urllib3
import os
import logging
from datetime import datetime 
from datetime import timedelta
from dotenv import load_dotenv
import time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Cargar variables de entorno desde el archivo .env
load_dotenv()
# Configuración
usuario = os.getenv('PI_USERNAME')
clave = os.getenv('PI_PASSWORD')
piserver = os.getenv('PI_SERVER')
tags_str = os.getenv('PI_TAGS')  # TAGS, SON CADA UNA DE LAS VARIABLES SEPARADAS POR COMAS

#url base para la API de PI Web
base_url = f'https://{piserver}/piwebapi'


# funcion para obtener el webid de un punto especifico
def obtener_webid(tag_name):
    try:
        url = f"{base_url}/points?path=\\\\{piserver}\\{tag_name}"
        response = requests.get(url, auth=HttpNtlmAuth(usuario, clave), verify=False)
        response.raise_for_status()
        return response.json()['WebId']
    except Exception as e:
        return e


# funcion para obtener los datos historicos con una fecha de inicio y fin
def obtener_datos_hist_pag(webid, start, end , max_per_request=800000):
    all_items = []
    current_start = start
    while True:
        url = f"{base_url}/streams/{webid}/recorded"
        params = {
            'startTime': current_start,
            'endTime': end,
            'maxCount': max_per_request,
            'boundaryType': 'Inside',     
        }
        try:
            response = requests.get(url, params=params, auth=HttpNtlmAuth(usuario, clave), verify=False)
            response.raise_for_status()  # Lanza un error si la respuesta no es 200 OK
        except Exception as e:
            return f"Error al obtener datos: {e}"
            break
        items = response.json().get('Items', [])
        if not items:
            
            break
        all_items.extend(items)
        # Obtener el último timestamp y continuar desde ahí
        last_timestamp = items[-1]['Timestamp']
        last_time = pd.to_datetime(last_timestamp) + pd.Timedelta(milliseconds=1)
        current_start = last_time.isoformat()
        time.sleep(0.1)  # Espera mínima para evitar saturar el servidor
        # Evitar bucles infinitos en caso de timestamps repetidos
        if len(items) < max_per_request:
            break
    df = pd.DataFrame([{'Timestamp': item['Timestamp'], 'Value': item['Value']} for item in all_items])
    return df


# Guardar los datos en un archivo CSV
def guardar_csv(df, tag_name):
    file_path=f"data/cruda/{tag_name}.csv"
    df.to_csv(file_path, index=False)
    
def generar_rangos_fechas(start_date_str, end_date_str, delta_dias=15):
    """
    Genera una lista de tuplas con rangos de fechas cada 15 días.
    """
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)
    rangos = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=delta_dias), end_date)
        rangos.append((current_start.isoformat(), current_end.isoformat()))
        current_start = current_end + timedelta(milliseconds=1)  # evita solapamientos exactos

    return rangos
if __name__ == '__main__':
    try:
        start_Time = '2024-09-09T00:00:00'
        end_Time = '2025-05-31T00:00:00'
        tags = [t.strip() for t in tags_str.split(",")]
        print(tags)
        for tag in tags:
            print(f"Procesando tag: {tag}")
            webid = obtener_webid(tag)
            if not webid:
                continue

            rangos = generar_rangos_fechas(start_Time, end_Time, delta_dias=15)
            df_total = pd.DataFrame()  # Para concatenar resultados
            for rango_inicio, rango_fin in rangos:
                print(f"[{tag}] Consultando rango {rango_inicio} a {rango_fin}")
                df = obtener_datos_hist_pag(webid, start=rango_inicio, end=rango_fin, max_per_request=50000)
                if df.empty:
                    continue
                df_total = pd.concat([df_total, df], ignore_index=True)
            
            if df_total.empty:
                print(f"[{tag}] No se encontraron datos en todo el rango especificado.")
                continue

            guardar_csv(df_total, tag)
    except Exception as e:
        print("Error:", e)

