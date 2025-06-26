# scheduler.py
from extract_data import obtener_webid, obtener_datos_hist_pag, generar_rangos_fechas
from store_data import store_to_postgres
from extract_config import TAGS
import pandas as pd
from datetime import datetime
import logging
import os

logging.basicConfig(filename="logs/etl_bronze.log", level=logging.INFO)

def main():
    start_time = '2024-09-09T00:00:00'
    end_time = '2025-06-16T00:00:00'
    
    for alias, tag_name in TAGS.items():
        try:
            print(f"Procesando: {alias}")
            webid = obtener_webid(tag_name)
            rangos = generar_rangos_fechas(start_time, end_time)
            print(f"[{alias}] Total rangos generados: {len(rangos)}")
            df_total = pd.DataFrame()
            for inicio, fin in rangos:
                #consultando rangos
                print(f"[{alias}] Consultando rango {inicio} a {fin}")
                try:
                    df = obtener_datos_hist_pag(webid, inicio, fin, max_per_request=50000)
                    if not df.empty:
                        store_to_postgres(df, alias)
                        logging.info(f"{alias} [{inicio} - {fin}] - {len(df)} registros.")
                    else:
                        print(f"[{alias}] No se encontraron datos en el rango {inicio} a {fin}.")
                except Exception as e:
                    logging.error(f"Error al consultar {alias} en rango {inicio} a {fin}: {str(e)}")
        except Exception as e:
            logging.error(f"Error en {alias}: {str(e)}")

if __name__ == "__main__":
    main()
