# store_data.py
import pandas as pd
from db import get_engine

def store_to_postgres(df, variable_name):
    if df.empty:
        return
    df['variable_name'] = variable_name
    df['timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df[['timestamp', 'value', 'variable_name']]
    df.columns = ['timestamp', 'value', 'variable_name']

    engine = get_engine()
    df.to_sql('raw_data', con=engine, if_exists='append', index=False)
