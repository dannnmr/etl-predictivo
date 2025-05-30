import pandas as pd
import os

def preprocesar_variable(path: str, resample_interval="30min") -> pd.DataFrame:
    df = pd.read_csv(path)
    nombre_archivo = os.path.splitext(os.path.basename(path))[0]

    # Renombrar columna
    df.rename(columns={"Value": nombre_archivo}, inplace=True)

    df[nombre_archivo] = pd.to_numeric(df[nombre_archivo], errors="coerce")
    df.dropna(subset=[nombre_archivo], inplace=True)

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], dayfirst=True)
    df.set_index("Timestamp", inplace=True)
    df = df.sort_index()
    df = df[~df.index.duplicated(keep='first')]

    if "position" in nombre_archivo.lower():
        df_resampled = df.resample(resample_interval).ffill()
    else:
        df_resampled = df.resample(resample_interval).mean()
        df_resampled[nombre_archivo] = df_resampled[nombre_archivo].interpolate(method='linear')

    return df_resampled
