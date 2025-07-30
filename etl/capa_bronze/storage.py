# etl/bronze/storage.py

import os
import pandas as pd
from config import BRONZE_DIR

def get_parquet_path(tag_alias):
    return os.path.join(BRONZE_DIR, f"{tag_alias}.parquet")


def leer_ultimo_timestamp(tag_alias):
    file_path = get_parquet_path(tag_alias)
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_parquet(file_path, engine='pyarrow')
        if df.empty:
            return None
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors="coerce")
        timestamp_max = df['timestamp'].max()
        timestamp_max = timestamp_max.floor('s')
        print(timestamp_max)
        return timestamp_max
        return timestamp_max
    except Exception as e:
        print(f"[ERROR] al leer parquet de {tag_alias}: {e}")
        return None


def guardar_parquet_append(df_nuevo, tag_alias):
    file_path = get_parquet_path(tag_alias)
    print(f"Guardando datos para {tag_alias} en {file_path}")
    if os.path.exists(file_path):
        try:
            df_existente = pd.read_parquet(file_path, engine='pyarrow')
            df_concat = pd.concat([df_existente, df_nuevo], ignore_index=True).drop_duplicates()
            df_concat.to_parquet(file_path, index=False)
        except Exception as e:
            print(f"[ERROR] al guardar {tag_alias}: {e}")
    else:
        df_nuevo.to_parquet(file_path, index=False)
    print(f"Datos guardados para {tag_alias}")


