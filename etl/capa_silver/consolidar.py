# etl/capa_silver/consolidar.py
import pandas as pd
from limpieza import limpiar_variable
from config import TODAS_LAS_VARIABLES

def cargar_todas_las_variables() -> dict:
    """
    Aplica limpieza a todas las variables definidas y las retorna en un diccionario.
    """
    data = {}
    for var in TODAS_LAS_VARIABLES:
        try:
            df = limpiar_variable(var)
            data[var] = df
            #print(data[var].head(3))  # Muestra las primeras 3 filas de cada variable
        except Exception as e:
            print(f"[ERROR] {var}: {e}")
    return data


def unir_variables(diccionario: dict) -> pd.DataFrame:
    """
    Une múltiples DataFrames por índice timestamp.
    """
    resultado = None
    for df in diccionario.values():
        if resultado is None:
            resultado = df
        else:
            resultado = resultado.join(df, how="outer")

    return resultado.sort_index() if resultado is not None else pd.DataFrame()


def generar_dataset_silver() -> pd.DataFrame:
    """
    Ejecuta la limpieza y consolidación de todas las variables.
    Devuelve un DataFrame unificado listo para guardar o cargar a BD.
    """
    print("[INFO] Cargando y limpiando variables Bronze...")
    variables_limpias = cargar_todas_las_variables()

    print("[INFO] Unificando variables por timestamp...")
    df_silver = unir_variables(variables_limpias)

    print("[INFO] Consolidacion completa. Total de registros:", len(df_silver))
    return df_silver
