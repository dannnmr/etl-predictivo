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
    

