import pandas as pd
import helper


# Constants

TABLE_NAME = 'trans_acu_servicios'
INDEX_NAME = 'key_trans_servicio'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()

# Extract

df_mensajeria_servicio = pd.read_sql_table('mensajeria_servicio', ryf_conn)
df_mensajeria_estado_servicio = pd.read_sql_table('mensajeria_estadosservicio', ryf_conn)
df_clientes_usuario = pd.read_sql_table('clientes_usuarioaquitoy', ryf_conn)


# Transform

df_mensajeria_estado_servicio = df_mensajeria_estado_servicio[[
    'servicio_id', 'observaciones', 'fecha', 'hora'
]].rename(
    columns={
        'observaciones': 'estado_servicio_observaciones',
        'fecha': 'estado_servicio_fecha',
        'hora': 'estado_servicio_hora'
    }
)

df_clientes_usuario = df_clientes_usuario[[
    'id', 'sede_id'
]]


# Merge

# Perform the LEFT JOIN operations
df_merged = (df_mensajeria_servicio.merge(
    df_mensajeria_estado_servicio,
    left_on='id',
    right_on='servicio_id',
    how='left'
).merge(
    df_clientes_usuario,
    left_on='usuario_id',
    right_on='id',
    how='left'
))

# Clean

# Update mensajero_id based on mensajero2_id and mensajero3_id
df_merged = df_merged.apply(helper.update_mensajero_ids, axis=1)

# Drop rows where mensajero_id is still None (these are the rows marked for dropping)
df_merged = df_merged.dropna(subset=['mensajero_id'])

# Drop columns
df_merged = df_merged.drop(columns=[
    'id_y', 'servicio_id', 'hora_visto_por_mensajero', 'visto_por_mensajero',
    'descripcion_multiples_origenes', 'tipo_pago_id', 'tipo_vehiculo_id', 'usuario_id',
    'hora_solicitud', 'fecha_deseada', 'mensajero2_id', 'mensajero3_id'
])

# Fill empty strings

df_merged['descripcion_cancelado'] = df_merged['descripcion_cancelado'].replace("", "N/A")
df_merged['descripcion_pago'] = df_merged['descripcion_pago'].replace("", "N/A")
df_merged['novedades'] = df_merged['novedades'].replace("", "N/A")

# Rename the columns
df_merged = df_merged.rename(
    columns={
        'id_x': 'servicio_id'
    }
)

# Drop the existing table if it exists
helper.load_data("etl_conn", df_merged, TABLE_NAME, INDEX_NAME)
