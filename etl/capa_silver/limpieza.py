"""limpieza de datos"""
# etl/capa_silver/limpieza.py

import pandas as pd
import os
from config import FRECUENCIA, BRONZE_DIR, VARIABLES_CONTINUAS, VARIABLES_DISCRETAS



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

    # print(f"timestamp valores nulos {df['timestamp'].isnull().sum()}") # Verificar si hay NaNs en timestamp
    #mostrar los registros con timestamp nulos
    df_nulos = df["timestamp"].isnull() # Verificar si hay NaNs en timestamp
    df_valores_nulos = df[nombre_variable].isnull() # Verificar si hay NaNs en timestamp
    
    print(f"{nombre_variable} valores nulos {df_valores_nulos.sum()}") # Verificar si hay NaNs en la variable
    # print(df[df_nulos]) # Mostrar registros con timestamp nulo
    #df_nulos= df[nombre_variable].isnull()


    df = df.dropna(subset=["timestamp", nombre_variable])
    df = df.set_index("timestamp").sort_values("timestamp") #

    

    # --- Lógica de Resampleo y Limpieza Mejorada ---

    if nombre_variable in VARIABLES_CONTINUAS: #
        # Para variables continuas, el promedio es adecuado para resamplear.
        df_resample = df.resample(FRECUENCIA).mean().copy() #
        for col in df_resample.columns: #
          print(f"despues de resample {col} nulos: {df_resample[col].isnull().sum()}")
          #imprimir valores nulos
          print(df_resample[df_resample[col].isnull()])
        # Interpolamos valores faltantes con método de tiempo, limitado a 3 periodos.
        df_resample = df_resample.interpolate(method="linear", limit=3) #
        print(df_resample.index)
        print(df_resample.index.freq) # Verificar la frecuencia del índice después del resampleo
        
        for col in df_resample.columns:
            print(f"despues de linear {col} nulos: {df_resample[col].isnull().sum()}")


    # elif nombre_variable in VARIABLES_CRITICAS: #
    #     # Para variables críticas, también promediamos para el resampleo.
    #     df_resample = df.resample(FRECUENCIA).mean()
    #     # Interpolamos si el % de nulos es bajo, si no dejamos NaN.
    #     if df_resample[nombre_variable].isna().mean() < 0.1: #
    #         df_resample = df_resample.interpolate(method="linear", limit=2) #
    #     # Si el % de nulos es alto, los NaNs se mantendrán.

    elif nombre_variable in VARIABLES_DISCRETAS: #
        df_resample = df.resample(FRECUENCIA).last() # Tomamos el último valor válido en el intervalo
        # Luego, rellenamos cualquier NaN que haya quedado con el último valor válido anterior.
        df_resample = df_resample.ffill() # Forward fill para rellenar NaNs
        # Opcional: Si quedaran NaNs al inicio, se pueden rellenar hacia atrás
        df_resample = df_resample.bfill() 
    
    else:
        # En caso de que una variable no esté clasificada, se puede definir un comportamiento por defecto
        print(f"[ADVERTENCIA] La variable '{nombre_variable}' no está clasificada. Se aplicará resampleo por media y ffill.")
        df_resample = df.resample(FRECUENCIA).mean()
        df_resample = df_resample.ffill()


    return df_resample[[nombre_variable]] #
