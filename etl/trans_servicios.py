import pandas as pd
import helper


# Constants

TABLE_NAME = 'trans_servicios'
INDEX_NAME = 'key_trans_servicio'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()

# Extract

df_mensajeria_servicio = pd.read_sql_table('mensajeria_servicio', ryf_conn)
df_clientes_usuario = pd.read_sql_table('clientes_usuarioaquitoy', ryf_conn)
df_mensajeria_estado_servicio = pd.read_sql_table('mensajeria_estadosservicio', ryf_conn)

# Transform

df_mensajeria_servicio = df_mensajeria_servicio[[
    'id', 'cliente_id', 'fecha_solicitud', 'hora_solicitud', 'usuario_id', 'mensajero_id', 'mensajero2_id', 'mensajero3_id'
]].rename(
    columns={
        'id': 'servicio_id'
    }
).sort_values(by='servicio_id', ascending=True)


df_clientes_usuario = df_clientes_usuario[[
    'id', 'sede_id'
]].rename(
    columns={
        'id': 'clientes_usuario_id'
    }
)


df_mensajeria_estado_servicio = df_mensajeria_estado_servicio[[
    'servicio_id', 'hora', 'fecha', 'estado_id'
]].rename(
    columns={
        'hora': 'estado_hora',
        'fecha': 'estado_fecha',
        'estado_id': 'estado_servicio_id'
    }
)

# Merge

# Perform the LEFT JOIN operations
df_merged = df_mensajeria_servicio.merge(
    df_clientes_usuario,
    how='left',
    left_on='usuario_id',
    right_on='clientes_usuario_id'
)

# Process service statuses for each service saving:
# estado_fecha_asignacion
# estado_hora_asignacion
# estado_fecha_recogida
# estado_hora_recogida
# estado_fecha_entrega
# estado_hora_entrega
# estado_fecha_cerrado
# estado_hora_cerrado
# And calculate the duration of each status from df_mensajeria_estado_servicio
# tiempo_minutos_asignacion
# tiempo_horas_asignacion
# tiempo_minutos_recogida
# tiempo_horas_recogida
# tiempo_minutos_entrega
# tiempo_horas_entrega
# tiempo_minutos_cerrado
# tiempo_horas_cerrado

# using the following rules:

# def process_service_statuses(row):
#     #find in df_mensajeria_estado_servicio all rows with servicio_id == row.servicio_id
#     df_service_statuses = df_mensajeria_estado_servicio[df_mensajeria_estado_servicio['servicio_id'] == row.servicio_id]
#     print(df_service_statuses.head(15))
#     exit()
# df_merged = df_merged.apply(process_service_statuses, axis=1)
#
# Clean

# Update mensajero_id based on mensajero2_id and mensajero3_id
df_merged = df_merged.apply(helper.update_mensajero_ids, axis=1)

# Drop rows where mensajero_id is still None (these are the rows marked for dropping)
df_merged = df_merged.dropna(subset=['mensajero_id'])

# Drop unnecessary columns
df_merged = df_merged.drop(columns=[
    'clientes_usuario_id', 'usuario_id', 'mensajero2_id', 'mensajero3_id'
])

# Drop the existing table if it exists
helper.load_data("etl_conn", df_merged, TABLE_NAME, INDEX_NAME)
