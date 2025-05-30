import os
from limpiar_datos import limpiar_y_formatear
from resamplear_datos import preprocesar_variable
from combinar_datos import combinar_todas_las_variables
import pandas as pd

input_folder = "data/cruda"
formatted_folder = "data/formateada"
resampled_folder = "data/resampleada"
processed_folder = "data/procesada"
output_file = "data/procesada/combined_data.csv"

os.makedirs(formatted_folder, exist_ok=True)
os.makedirs(resampled_folder, exist_ok=True)
os.makedirs("data/procesada", exist_ok=True)

# Paso 1: limpieza
for archivo in os.listdir(input_folder):
    if archivo.endswith(".csv"):
        ruta = os.path.join(input_folder, archivo)
        df = pd.read_csv(ruta)
        df_clean = limpiar_y_formatear(df, archivo)
        df_clean.to_csv(os.path.join(formatted_folder, archivo.replace(".csv", "_formatted.csv")), index=False)

# Paso 2: resamplear
for archivo in os.listdir(formatted_folder):
    if archivo.endswith(".csv"):
        ruta = os.path.join(formatted_folder, archivo)
        df_resampled = preprocesar_variable(ruta)
        df_resampled.to_csv(os.path.join(resampled_folder, archivo.replace(".csv", "_resampleado.csv")))


# Paso 3: combinar
combinado = combinar_todas_las_variables(resampled_folder, output_file)
print("Transformación completa. Archivo combinado guardado.")
