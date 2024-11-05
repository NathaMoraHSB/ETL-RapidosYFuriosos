import pandas as pd
import helper

# Constants

TABLE_NAME = 'dim_hora'
INDEX_NAME = 'key_dim_hora'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()


# Crear un rango de horas para un solo día, con un paso de una hora

horas = pd.date_range(start='00:00', end='23:59', freq='1min', inclusive='left')

# Crear el DataFrame de la dimensión hora
dim_hora = pd.DataFrame({
    "hora_completa": horas.time  # Solo la parte de la hora (HH:MM:SS)
})

# Añadir columnas derivadas

dim_hora["hora_id"] = dim_hora["hora_completa"].apply(lambda x: int(x.strftime('%H%M')))  # ID de la hora en formato HHMM
dim_hora["hora"] = dim_hora["hora_completa"].apply(lambda x: x.strftime('%H'))  # Solo la hora (00, 01, ..., 23)
dim_hora["minuto"] = dim_hora["hora_completa"].apply(lambda x: x.strftime('%M'))  # Solo el minuto (00, 01, ..., 59)
dim_hora["descripcion"] = dim_hora["hora_completa"].apply(lambda x: x.strftime('%H:%M'))  # Descripción en formato HH:MM


# Load
helper.load_data("etl_conn", dim_hora, TABLE_NAME, INDEX_NAME)
