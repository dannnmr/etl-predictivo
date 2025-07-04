# etl/capa_silver/loader.py

import os
import pandas as pd
from sqlalchemy import create_engine
from config import SILVER_DIR

def guardar_en_parquet(df: pd.DataFrame, nombre: str = "silver_data") -> str:
    """Guarda el DataFrame en formato Parquet en la carpeta SILVER_DIR.
    Si no se especifica nombre, usa "silver_data" como predeterminado.
    Devuelve la ruta del archivo guardado.
    """
    path = os.path.join(SILVER_DIR, f"{nombre}.parquet")
    df.to_parquet(path, index=True)
    print(f"[INFO] Datos guardados en Parquet: {path}")
    return path


def guardar_en_postgresql(df: pd.DataFrame, nombre_tabla: str, conn_str: str):
    """
    Guarda el DataFrame en una base de datos PostgreSQL usando SQLAlchemy.
    """
    try:
        engine = create_engine(conn_str)
        df.to_sql(nombre_tabla, con=engine, if_exists="replace", index=True)
        print(f"[INFO] Datos cargados en PostgreSQL: tabla {nombre_tabla}")
    except Exception as e:
        print(f"[ERROR] No se pudo guardar en PostgreSQL: {e}")
