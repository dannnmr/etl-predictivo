"""
LIMPIEZA DE DATOS
Script para la limpieza de datos en la capa Silver de una arquitectura Medallion.
Este script carga datos desde la capa Bronze, estandariza los timestamps,
y aplica estrategias de resampleo y manejo de valores nulos
diferenciadas para variables continuas y discretas.

Consideraciones:
- FRECUENCIA se establece en "15min" en config.py.
- Para variables continuas, se aplica interpolación lineal limitada. Los NaNs restantes
  después de la interpolación SON ELIMINADOS (dropna).
- TAP_POSICION es tratada como discreta y se rellena con ffill/bfill.
"""

import numpy as np
import pandas as pd
import os
from config import FRECUENCIA, BRONZE_DIR, VARIABLES_CONTINUAS, VARIABLES_DISCRETAS, LIMIT_INTERPOLACION, LIMITES


def limpiar_variable(nombre_variable: str) -> pd.DataFrame:
    """
    Carga y limpia cada variable desde Bronze. Devuelve un DataFrame con index timestamp y 1 columna.
    """
    ruta = os.path.join(BRONZE_DIR, f"{nombre_variable}.parquet") 
    if not os.path.exists(ruta): 
        raise FileNotFoundError(f"No se encontró el archivo: {ruta}") 
    df = pd.read_parquet(ruta) 
    df = df.rename(columns={"value": nombre_variable}) 
    
    # Aplicar hard clamping según los límites definidos
    if nombre_variable in LIMITES:
        limites = LIMITES[nombre_variable]
        if limites.get("min") is not None:
            df.loc[df[nombre_variable] <= limites["min"], nombre_variable] = np.nan
        if limites.get("max") is not None:
            df.loc[df[nombre_variable] > limites["max"], nombre_variable] = np.nan
            
    # Convertir a string, eliminar Z y cortar microsegundos si son >6
    df["timestamp"] = (
        df["timestamp"]
        .astype(str)
        .str.replace(r"(\.\d{6})\d+", r"\1", regex=True)
        .str.replace("Z", "")
    )
    # Convertir a datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")  
    # Eliminar zona horaria y redondear el timestamp a segundos (sin microsegundos)
    df["timestamp"] = df["timestamp"].dt.tz_localize(None).dt.floor("s")
    df = df.set_index("timestamp").sort_values("timestamp") #
    
    
    df_resample = pd.DataFrame()  # Inicializar para evitar UnboundLocalError
    
    # --- Lógica de Resampleo y Limpieza Mejorada ---
    if nombre_variable in VARIABLES_CONTINUAS: #
        # Para variables continuas, el promedio es adecuado para resamplear.}
        # Interpolamos valores faltantes con método de tiempo, limitado a 3 periodos.
        df_resample = df.resample(FRECUENCIA).mean() #
        df_resample = df_resample.interpolate(method="linear", limit_direction="both", limit_area="inside", limit=LIMIT_INTERPOLACION) 
        
    elif nombre_variable in VARIABLES_DISCRETAS: #
        df_resample = df.resample(FRECUENCIA).last() # Tomamos el último valor válido en el intervalo
        # Luego, rellenamos cualquier NaN que haya quedado con el último valor válido anterior.
        df_resample[nombre_variable] = df_resample[nombre_variable].ffill() # Forward fill para rellenar NaNs
        df_resample[nombre_variable] = df_resample[nombre_variable].bfill()
    else:
        # En caso de que una variable no esté clasificada, se puede definir un comportamiento por defecto
        print(f"[ADVERTENCIA] La variable '{nombre_variable}' no esta clasificada. Se aplicara resampleo por media y ffill.")
        df_resample = df.resample(FRECUENCIA).mean()
        df_resample[nombre_variable] = df_resample[nombre_variable].ffill()


    #print(f"[INFO] '{nombre_variable}' despues de procesar: {df_resample[nombre_variable].sum()}")  # Debería ser 0
    return df_resample[[nombre_variable]]












