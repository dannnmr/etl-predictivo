import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import logging
from datetime import datetime

# Configuración de log
log_name = datetime.now().strftime("logs/carga_%Y%m%d_%H%M%S.log")
logging.basicConfig(filename=log_name, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Cargar variables de entorno
load_dotenv()
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# Ruta al archivo procesado
input_file = "data/procesada/combined_data.csv"

def cargar_a_postgres(input_file: str, table_name: str = "datos_transformados"):
    try:
        # Leer archivo CSV
        df = pd.read_csv(input_file)
        
        # Asegurar que 'Timestamp' sea datetime y establecerlo como índice
        if "Timestamp" in df.columns:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
            df.set_index("Timestamp", inplace=True)
            df.sort_index(inplace=True)

        # Crear motor de conexión a PostgreSQL
        engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        
        # Cargar datos a PostgreSQL
        df.to_sql(table_name, engine, if_exists="replace", index=True)
        logging.info(f"Datos cargados exitosamente en la tabla {table_name} de la base de datos {db_name}.")
        print(f"Datos cargados exitosamente en la tabla {table_name}.")

    except Exception as e:
        logging.error(f"Error al cargar datos: {e}")
        print(f"Error al cargar datos: {e}")

if __name__ == "__main__":
    cargar_a_postgres(input_file)
