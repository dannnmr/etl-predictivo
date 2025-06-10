import os
import pandas as pd

def combinar_todas_las_variables(carpeta: str, output_file: str):
    archivos = [f for f in os.listdir(carpeta) if f.endswith(".csv")]
    dataframes = []

    for archivo in archivos:
        path = os.path.join(carpeta, archivo)
        df = pd.read_csv(path)

        # Convertir Timestamp a datetime si existe
        if 'Timestamp' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
            df.set_index('Timestamp', inplace=True)
        else:
            raise ValueError(f"El archivo {archivo} no contiene la columna 'Timestamp'.")

        # Agregar el DataFrame a la lista
        # Verificar columnas de datos (debería haber solo una)
        columnas_datos = [col for col in df.columns]
        if len(columnas_datos) != 1:
            raise ValueError(f"El archivo {archivo} tiene más de una columna de datos: {columnas_datos}")

        # Renombrar la columna de datos con el nombre del archivo (sin extensión)
        nombre_variable = os.path.splitext(archivo)[0]
        df.rename(columns={columnas_datos[0]: nombre_variable}, inplace=True)
        dataframes.append(df)
        

    # Combinar todos los DataFrames por índice (Timestamp)
    combinado = pd.concat(dataframes, axis=1)

    # Eliminar duplicados en el índice si existieran
    combinado = combinado[~combinado.index.duplicated(keep='first')]

    # Ordenar por Timestamp
    combinado.sort_index(inplace=True)

    # Nombrar el índice para exportación
    combinado.index.name = 'Timestamp'

    # Exportar a CSV
    combinado.to_csv(output_file, date_format='%Y-%m-%d %H:%M:%S')

    print(f"Archivo combinado exportado correctamente en: {output_file}")
    return combinado
