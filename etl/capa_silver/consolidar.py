# etl/capa_silver/consolidar.py
import math
import pandas as pd
from limpieza import limpiar_variable
import numpy as np
from config import TODAS_LAS_VARIABLES, VOLTAJE_NOMINAL, VARIABLES_CONTINUAS
# Importar las clases necesarias para KNN Imputation
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler



def cargar_todas_las_variables() -> dict:
    """
    Aplica limpieza a todas las variables definidas y las retorna en un diccionario.
    """
    data = {}
    for var in TODAS_LAS_VARIABLES:
        try:
            df = limpiar_variable(var)
            data[var] = df
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


def imputar_voltaje_con_ruido(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reemplaza los valores NaN en la columna 'voltaje' del DataFrame según el tap_position.
    Usa el valor nominal del tap_position más un ruido basado en la desviación estándar
    de los valores reales de 'voltaje' para ese tap_position.
    """
    # Asegurar que los tipos sean correctos
    df = df.copy()
    df['tap_position'] = df['tap_position'].astype(float)
    # Calcular desviaciones estándar por tap_position donde haya datos válidos
    std_por_tap = df.dropna(subset=['voltaje']).groupby('tap_position')['voltaje'].std()

    # Procesar filas con NaN en voltaje
    for idx, row in df[df['voltaje'].isna()].iterrows():
        tap = row['tap_position']
        volt_nominal = VOLTAJE_NOMINAL.get(tap)
        if volt_nominal is None:
            continue  # Tap fuera de rango

        std = std_por_tap.get(tap, 0)  # Si no hay std para ese tap, usar 0 (sin ruido)
        ruido = np.random.normal(0, std)
        df.at[idx, 'voltaje'] = volt_nominal/(1000*math.sqrt(3)) + ruido
    return df

# def estimar_tap_position(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estima el tap_position más probable para filas donde tap_position es NaN,
    usando el valor de voltaje más cercano al nominal de cada tap.
    """
    df = df.copy()
    # Invertir el diccionario a un DataFrame para facilitar cálculos
    tap_df = pd.DataFrame({
        'tap_position': list(VOLTAJE_NOMINAL.keys()),
        'volt_nominal': list(VOLTAJE_NOMINAL.values())
    })

    missing_tap = df[df['tap_position'].isna() & df['voltaje'].notna()]

    for idx, row in missing_tap.iterrows():
        volt = row['voltaje']*(1000*math.sqrt(3))  # Convertir a voltaje nominal

        # Calcular diferencia con cada voltaje nominal
        tap_df['diferencia'] = (tap_df['volt_nominal'] - volt).abs()

        # Elegir el tap con menor diferencia
        tap_estimado = tap_df.loc[tap_df['diferencia'].idxmin(), 'tap_position']

        df.at[idx, 'tap_position'] = tap_estimado

    return df

def imputacion_knn(df: pd.DataFrame, n_vecinos: int = 5) -> pd.DataFrame:
    """
    Aplica imputación KNN a variables continuas (excepto voltaje) con estandarización.
    Incluye tap_position como feature auxiliar si existe.
    """
    df = df.copy()
    columnas_objetivo = [var for var in VARIABLES_CONTINUAS if var != "voltaje" and var in df.columns]
    features = columnas_objetivo.copy()

    if "tap_position" in df.columns:
        features.append("tap_position")

    subdf = df[features]
    scaler = StandardScaler()
    scaled = scaler.fit_transform(subdf)

    imputer = KNNImputer(n_neighbors=n_vecinos)
    imputado_scaled = imputer.fit_transform(scaled)
    imputado = scaler.inverse_transform(imputado_scaled)

    df[features] = pd.DataFrame(imputado, columns=features, index=df.index)
    if "tap_position" in df.columns:
        df["tap_position"] = df["tap_position"].round().astype(int)

    print("[INFO] Imputación KNN completada sobre:", features)
    return df

def generar_dataset_silver() -> pd.DataFrame:
    """
    Ejecuta la limpieza y consolidación de todas las variables,
    aplica imputación basada en dominio y devuelve un DataFrame unificado.
    """
    print("[INFO] Cargando y limpiando variables Bronze...")
    variables_limpias = cargar_todas_las_variables()
    df_silver = unir_variables(variables_limpias)

    if 'voltaje' in df_silver.columns and 'tap_position' in df_silver.columns:
        df_silver = imputar_voltaje_con_ruido(df_silver)
    else:
        print("Se omitió la imputación basada en dominio.")

    # Imputación KNN robusta
    df_silver = imputacion_knn(df_silver)

    return df_silver

generar_dataset_silver()






