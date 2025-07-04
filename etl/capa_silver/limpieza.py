"""limpieza de datos"""
# etl/capa_silver/limpieza.py

import pandas as pd
import os
from config import FRECUENCIA, BRONZE_DIR, VARIABLES_CONTINUAS, VARIABLES_CRITICAS, VARIABLES_DISCRETAS


# def limpiar_variable(nombre_variable: str) -> pd.DataFrame:
    # """
    # Carga y limpia cada variable desde Bronze. Devuelve un DataFrame con index timestamp y 1 columna.
    # """
    # ruta = os.path.join(BRONZE_DIR, f"{nombre_variable}.parquet")
    # if not os.path.exists(ruta):
    #     raise FileNotFoundError(f"No se encontró el archivo: {ruta}")

    # df = pd.read_parquet(ruta)
    # # Renombramos la columna de datos
    # df = df.rename(columns={"value": nombre_variable})
    # df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    
    # df = df.dropna(subset=["timestamp", nombre_variable])
    # df = df.sort_values("timestamp").set_index("timestamp")

    # df_resample = df.resample(FRECUENCIA).mean()

    # if nombre_variable in VARIABLES_CONTINUAS:
    #     df_resample = df_resample.interpolate(method="time", limit=3)

    # elif nombre_variable in VARIABLES_CRITICAS:
    #     # Interpolamos si el % de nulos es bajo, si no dejamos NaN
    #     if df_resample[nombre_variable].isna().mean() < 0.1:
    #         df_resample = df_resample.interpolate(method="time", limit=2)
    # elif nombre_variable in VARIABLES_DISCRETAS:
    #     df_resample = df_resample.ffill()

    # return df_resample[[nombre_variable]]



def limpiar_variable(nombre_variable: str) -> pd.DataFrame:
    """
    Carga y limpia cada variable desde Bronze. Devuelve un DataFrame con index timestamp y 1 columna.
    """
    ruta = os.path.join(BRONZE_DIR, f"{nombre_variable}.parquet") #
    if not os.path.exists(ruta): #
        raise FileNotFoundError(f"No se encontró el archivo: {ruta}") #

    df = pd.read_parquet(ruta) #
    # Renombramos la columna de datos
    df = df.rename(columns={"value": nombre_variable}) #
    # df["timestamp"] = df["timestamp"].astype(str).str.replace(r'(.\d{6})\d+', r'\1', regex=True) #esto es para quitar los microsegundos 
    # df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce") #
    # # Convertir de UTC a hora local y remover información de zona horaria
    # df["timestamp"] = df["timestamp"].dt.tz_convert(None)  
    # --- Limpieza del timestamp ---
# Convertir a string, eliminar Z y cortar microsegundos si son >6
    df["timestamp"] = (
        df["timestamp"]
        .astype(str)
        .str.replace(r"(\.\d{6})\d+", r"\1", regex=True)
        .str.replace("Z", "")
    )

    # Convertir a datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")  #


    # Eliminar zona horaria y redondear el timestamp a segundos (sin microsegundos)
    df["timestamp"] = df["timestamp"].dt.tz_localize(None).dt.floor("s")

    print(f"timestamp valores nulos {df['timestamp'].isnull().sum()}") # Verificar si hay NaNs en timestamp
    #mostrar los registros con timestamp nulos
    df_nulos = df["timestamp"].isnull() # Verificar si hay NaNs en timestamp
    print(df[df_nulos]) # Mostrar registros con timestamp nulo
    #df_nulos= df[nombre_variable].isnull()


    df = df.dropna(subset=["timestamp", nombre_variable])
    df = df.sort_values("timestamp").set_index("timestamp") #

    # --- Lógica de Resampleo y Limpieza Mejorada ---

    if nombre_variable in VARIABLES_CONTINUAS: #
        # Para variables continuas, el promedio es adecuado para resamplear.
        df_resample = df.resample(FRECUENCIA).mean() #
        # Interpolamos valores faltantes con método de tiempo, limitado a 3 periodos.
        df_resample = df_resample.interpolate(method="time", limit=3) #

    elif nombre_variable in VARIABLES_CRITICAS: #
        # Para variables críticas, también promediamos para el resampleo.
        df_resample = df.resample(FRECUENCIA).mean()
        # Interpolamos si el % de nulos es bajo, si no dejamos NaN.
        if df_resample[nombre_variable].isna().mean() < 0.1: #
            df_resample = df_resample.interpolate(method="time", limit=2) #
        # Si el % de nulos es alto, los NaNs se mantendrán.

    elif nombre_variable in VARIABLES_DISCRETAS: #
        df_resample = df.resample(FRECUENCIA).last() # Tomamos el último valor válido en el intervalo
        # Luego, rellenamos cualquier NaN que haya quedado con el último valor válido anterior.
        df_resample = df_resample.ffill() # Forward fill para rellenar NaNs
        # Opcional: Si quedaran NaNs al inicio, se pueden rellenar hacia atrás backward fill.
        df_resample = df_resample.bfill() 
    
    else:
        # En caso de que una variable no esté clasificada, se puede definir un comportamiento por defecto
        # Por ejemplo, resamplear con la media y luego un forward fill.
        print(f"[ADVERTENCIA] La variable '{nombre_variable}' no está clasificada. Se aplicará resampleo por media y ffill.")
        df_resample = df.resample(FRECUENCIA).mean()
        df_resample = df_resample.ffill()


    return df_resample[[nombre_variable]] #
