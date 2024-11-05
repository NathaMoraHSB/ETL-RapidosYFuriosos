import pandas as pd
import helper

# Constants

TABLE_NAME = 'dim_novedad'
INDEX_NAME = 'key_dim_novedad'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()


# Extract

df_novedad = pd.read_sql_table('mensajeria_novedadesservicio', ryf_conn)
df_tipo_novedad = pd.read_sql_table('mensajeria_tiponovedad', ryf_conn)

# Transform

df_novedad = df_novedad[['id', 'descripcion', 'fecha_novedad', 'tipo_novedad_id']].rename(columns={'id': 'novedad_id', 'descripcion': 'novedad_descripcion', 'fecha_novedad': 'novedad_fecha'})
df_tipo_novedad = df_tipo_novedad[['id', 'nombre']].rename(columns={'id': 'tipo_novedad_id', 'nombre': 'tipo_novedad_nombre'})

# Clean

# Fill empty strings
df_novedad['novedad_descripcion'] = df_novedad['novedad_descripcion'].fillna("N/A")
df_novedad['novedad_descripcion'] = df_novedad['novedad_descripcion'].replace("", "N/A")
# Merge

# Perform the LEFT JOIN operations

df_dim_novedad = pd.merge(df_novedad, df_tipo_novedad, on='tipo_novedad_id', how='left')

# Clean

# Drop columns that are not needed

df_dim_novedad = df_dim_novedad.drop(columns=['tipo_novedad_id'])

# Load

helper.load_data("etl_conn", df_dim_novedad, TABLE_NAME, INDEX_NAME)
