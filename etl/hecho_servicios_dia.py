import pandas as pd
import helper


# Constants

TABLE_NAME = 'hecho_servicios_dia'
INDEX_NAME = 'key_hecho_servicios_dia'

# Establish connections

etl_conn = helper.get_etl_conn()


# Extract

# Load transformed data
df_trans_servicios = pd.read_sql_table('trans_servicios', etl_conn).sort_values(
    by='key_trans_servicio',
    ascending=True)

# Load dimensions

df_dim_cliente = pd.read_sql_table('dim_cliente', etl_conn)
df_dim_mensajero = pd.read_sql_table('dim_mensajero', etl_conn)
df_dim_sede = pd.read_sql_table('dim_sede', etl_conn)
df_dim_fecha = pd.read_sql_table('dim_fecha', etl_conn)
df_dim_hora = pd.read_sql_table('dim_hora', etl_conn)

# Transform

# Remove unnecessary columns

df_merged = df_trans_servicios.drop(columns=[
    'estado_fecha_asignacion', 'estado_hora_asignacion', 'tiempo_minutos_asignacion', 'tiempo_horas_asignacion',
    'estado_fecha_recogida', 'estado_hora_recogida', 'tiempo_minutos_recogida', 'tiempo_horas_recogida',
    'estado_fecha_entrega', 'estado_hora_entrega', 'tiempo_minutos_entrega', 'tiempo_horas_entrega',
    'estado_fecha_cerrado', 'estado_hora_cerrado', 'tiempo_minutos_cerrado', 'tiempo_horas_cerrado',
    'total_tiempo_minutos', 'total_tiempo_horas'
])

print(df_merged.columns)

# need to answer the following question

# Merge with dim_fecha

df_merged['fecha_solicitud'] = pd.to_datetime(df_merged['fecha_solicitud'])
df_merged = pd.merge(
    df_merged,
    df_dim_fecha[['key_dim_fecha', 'fecha_id']],
    left_on=df_merged['fecha_solicitud'].dt.strftime('%Y%m%d').astype(int),
    right_on='fecha_id').drop(columns=['fecha_id', 'fecha_solicitud']).rename(columns={'key_dim_fecha': 'key_dim_fecha_solicitud'})

df_merged = df_merged.groupby('key_dim_fecha_solicitud').size().reset_index(name='numero_servicios')


# Load
helper.load_data("etl_conn", df_merged, TABLE_NAME, INDEX_NAME)
