import os
import pandas as pd

def combinar_todas_las_variables(carpeta: str, output_file: str):
    archivos = [f for f in os.listdir(carpeta) if f.endswith(".csv")]
    dataframes = []
    rangos = []

    for archivo in archivos:
        path = os.path.join(carpeta, archivo)
        df = pd.read_csv(path)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
        df.set_index("Timestamp", inplace=True)
        df.sort_index(inplace=True)

        rangos.append((df.index.min(), df.index.max()))
        var_name = df.columns[0]
        df.rename(columns={var_name: os.path.splitext(archivo)[0]}, inplace=True)

        dataframes.append(df)

    start, end = min(r[0] for r in rangos), max(r[1] for r in rangos)
    index_maestro = pd.date_range(start=start, end=end, freq="30min")

    combinado = pd.DataFrame(index=index_maestro)
    for df in dataframes:
        combinado[df.columns[0]] = df.reindex(index_maestro)[df.columns[0]]

    combinado.fillna(0, inplace=True)
    combinado.to_csv(output_file)
    return combinado
