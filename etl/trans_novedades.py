import pandas as pd
import helper


# Constants

TABLE_NAME = 'trans_novedades'
INDEX_NAME = 'key_trans_novedad'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()

# Extract

df_mensajeria_novedad_servicio = pd.read_sql_table('mensajeria_novedadesservicio', ryf_conn)

# Transform

df_mensajeria_novedad_servicio = df_mensajeria_novedad_servicio[[
    'id', 'fecha_novedad', 'tipo_novedad_id', 'descripcion', 'servicio_id', 'mensajero_id'
]]

# Merge

# Perform the LEFT JOIN operations
df_merged = df_mensajeria_novedad_servicio
# Clean

# Drop columns
# df_merged = df_merged.drop(columns=[])

# Fill empty strings

df_merged['descripcion'] = df_merged['descripcion'].replace("", "N/A")
df_merged['descripcion'] = df_merged['descripcion'].fillna("N/A")


# Drop the existing table if it exists
helper.load_data("etl_conn", df_merged, TABLE_NAME, INDEX_NAME)
