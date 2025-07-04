# etl/capa_silver/main.py

from consolidar import generar_dataset_silver
from loader import guardar_en_parquet, guardar_en_postgresql
import os
from dotenv import load_dotenv
load_dotenv()
#base de datos

if __name__ == "__main__":
    print("Iniciando capa silver")
    df_silver = generar_dataset_silver()

    if not df_silver.empty:
        guardar_en_parquet(df_silver)

        # Carga en PostgreSQL si la variable de entorno está definida
        
        load_dotenv()
        CONN_STR = os.getenv("POSTGRES_URI")
        if CONN_STR:
            print("[INFO] Guardando en PostgreSQL...")
            guardar_en_postgresql(df_silver, "silver_transformador", CONN_STR)
        else:
            print("[ADVERTENCIA] POSTGRES_URI no está definida. Se omitió la carga a PostgreSQL.")

    else:
        print("[ADVERTENCIA] El DataFrame Silver está vacío. No se guardó nada.")



