import os
from limpiar_datos import limpiar_y_formatear
from resamplear_datos import preprocesar_variable
from combinar_datos import combinar_todas_las_variables
from filtrar_datos import filtrar_datos
import pandas as pd

input_folder = "data/cruda"
formatted_folder = "data/formateada"
processed_folder = "data/procesada"
output_file = "data/procesada/combined_data.csv"

os.makedirs(formatted_folder, exist_ok=True)
# os.makedirs(resampled_folder, exist_ok=True)
os.makedirs("data/procesada", exist_ok=True)

# Paso 1: limpieza
for archivo in os.listdir(input_folder):
    if archivo.endswith(".csv"):
        ruta = os.path.join(input_folder, archivo)
        df = pd.read_csv(ruta)

        # Paso 1: limpieza
        df_clean = limpiar_y_formatear(df, archivo)

        # Guardar temporalmente en memoria y pasar al resampleo
        # Paso 2: resamplear
        nombre_archivo = archivo.replace(".csv", "_resampleado.csv")
        path_temporal = os.path.join(formatted_folder, "temp.csv")
        df_clean.to_csv(path_temporal, index=False)  # se guarda solo para usar el mismo flujo

        df_resampled = preprocesar_variable(path_temporal)

        # Guardar directamente en la carpeta final
        df_resampled.to_csv(os.path.join(formatted_folder, nombre_archivo))

        os.remove(path_temporal)  # limpiar archivo temporal

# Paso 3: combinar
combinado = combinar_todas_las_variables(formatted_folder, output_file)
df_combined = pd.read_csv(output_file, index_col=0, parse_dates=True)

# Exportar dataset limpio y filtrado
df_combined.to_csv('data/procesada/combined_data_ready.csv', date_format='%Y-%m-%d %H:%M:%S')
print("Archivo combinado y filtrado")

