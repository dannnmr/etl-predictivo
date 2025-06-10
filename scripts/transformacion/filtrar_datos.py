import pandas as pd

def filtrar_datos(df):
    # Renombrar columnas para mejorar legibilidad
    df.rename(columns={
        'TR1.Ambient temperature_resampleado': 'temperatura_ambiente',
        'TR1.Bubbling temperature_resampleado': 'temperatura_burbujeo',
        'TR1.Hot spot temperature_resampleado': 'temperatura_punto_caliente',
        'TR1.Load current LV Ph 2_resampleado': 'corriente_carga',
        'TR1.Moisture of insulation paper_resampleado': 'humedad_papel_aislante',
        'TR1.Oil temperature OLTC 1_resampleado': 'temperatura_aceite_OLTC',
        'TR1.Power (apparent power) 1m_resampleado': 'potencia_aparente',
        'TR1.Tap Position_resampleado': 'posicion_tap',
        'TR1.Top oil temperature_resampleado': 'temperatura_aceite',
        'TR1.Voltage (phase - ground) HV Ph 2_resampleado':'voltage'
    }, inplace=True)

    # Filtrar outliers basados en criterios técnicos
    df = df[df['temperatura_burbujeo'] >= 156]
    df = df[df['corriente_carga'] <= 1265]
    df = df[df['potencia_aparente'] <= 51]
    df = df[df['voltage'] >= 124]
    df = df[df['voltage'] <= 137]

    # Eliminar duplicados y valores nulos (si surgieron)
    df.dropna(inplace=True)
    df = df.drop_duplicates()

    df = df[~df.index.duplicated(keep='first')]

    return df
