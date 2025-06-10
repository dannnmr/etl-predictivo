import pandas as pd

def limpiar_y_formatear(df: pd.DataFrame, nombre_archivo: str) -> pd.DataFrame:
    df = df.copy()

    # Normalizar Timestamp
    if "Timestamp" in df.columns:
        df["Timestamp"] = df["Timestamp"].astype(str).str.replace("Z", "", regex=False)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce", utc=True)
        df["Timestamp"] = df["Timestamp"].dt.tz_convert(None)

    # Normalizar Value
    if "Value" in df.columns:
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
        df = df.dropna(subset=["Value"])  # elimina filas donde Value no es válido

   
    # Reordenar columnas (opcional)
    #sirve para
    df = df[["Timestamp", "Value"]]
    
    return df

