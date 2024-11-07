import pandas as pd
import helper

# Constants

TABLE_NAME = 'dim_hora'
INDEX_NAME = 'key_dim_hora'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()


# Crear un rango de horas para un solo día, con un paso de una hora

horas = pd.date_range(start='2024-01-01 00:00:00', end='2024-01-01 23:00:00', freq='h')

# Crear el DataFrame de la dimensión hora
dim_hora = pd.DataFrame({
    "hora": horas.strftime('%H')  # Solo la hora (00, 01, ..., 23)
})

# Load
helper.load_data("etl_conn", dim_hora, TABLE_NAME, INDEX_NAME)
