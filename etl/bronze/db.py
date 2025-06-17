# db.py
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    print(f"DB URL: postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

    return create_engine(url)
 # Test if the engine can be created:
try:
    engine = get_engine()
    with engine.connect() as connection:
        
        print("Database connection successful.")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    
#consultar los datos de la tabla
def get_data(table_name):
    engine = get_engine()
    with engine.connect() as connection:
        query = f"SELECT * FROM {table_name} WHERE variable_name = 'tap_position' ORDER BY timestamp LIMIT 100;"
        result = connection.execute(query)
        data = result.fetchall()
        return data

# validate_data.py
from db import get_engine
import pandas as pd

def validar_tap_position():
    try:
        engine = get_engine()
        query = """
            SELECT timestamp, value
            FROM raw_data
            WHERE variable_name = 'tap_position'
            ORDER BY timestamp
            LIMIT 100;
        """
        df = pd.read_sql(query, con=engine)

        if df.empty:
            print("❌ No se encontraron registros para 'tap_position'.")
        else:
            print(f"✅ Se encontraron {len(df)} registros para 'tap_position'.")
            print(df.head())

    except Exception as e:
        print(f"⚠️ Error durante la validación: {e}")

validar_tap_position()