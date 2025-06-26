# etl/bronze/main.py

from etl.capa_bronze.config import TAGS, START_TIME, END_TIME
from etl.capa_bronze.extract import obtener_webid, generar_rangos_fechas, obtener_datos_hist_pag
from etl.capa_bronze.storage import leer_ultimo_timestamp, guardar_parquet_append
import pandas as pd
# main.py

"""Este script se encarga de extraer datos históricos de un servidor PI, procesarlos y almacenarlos en formato Parquet.
Se basa en la configuración definida en config.py y utiliza funciones de extract.py y storage.py para realizar las operaciones necesarias."""

def sanitize_value(val):
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return val
    return None

def extraer_datos_actualizados(tag_alias, tag_path):
    try:
        webid = obtener_webid(tag_path)
        fecha_inicio = leer_ultimo_timestamp(tag_alias)
        if fecha_inicio:
            fecha_inicio += pd.Timedelta(milliseconds=1)
            print(f"Reanudando desde {fecha_inicio} para {tag_alias}")
        else:
            fecha_inicio = pd.to_datetime(START_TIME)
            print(f"Extrayendo desde cero para {tag_alias}")

        rangos = generar_rangos_fechas(str(fecha_inicio), END_TIME, delta_dias=15)
        df_total = pd.DataFrame()


        for inicio, fin in rangos:
            print(f"Rango: {inicio} -> {fin}")
            df = obtener_datos_hist_pag(webid, inicio, fin)
            if not df.empty:
                df["value"] = df["value"].apply(sanitize_value)
                df_total = pd.concat([df_total, df], ignore_index=True)

        if not df_total.empty:
            guardar_parquet_append(df_total, tag_alias)
        else:
            print(f"No se encontraron datos nuevos para {tag_alias}.")

    except Exception as e:
        print(f"[ERROR] {tag_alias}: {e}")


if __name__ == "__main__":
    for tag_alias, tag_path in TAGS.items():
        print(f"\n Procesando: {tag_alias} ")
        extraer_datos_actualizados(tag_alias, tag_path)
