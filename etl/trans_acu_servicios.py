import pandas as pd
import helper

# Establish connections
ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()

# Extract
df_mensajeria_servicio = pd.read_sql_table('mensajeria_servicio', ryf_conn)
df_mensajeria_estado_servicio = pd.read_sql_table('mensajeria_estadosservicio', ryf_conn)
df_clientes_usuario = pd.read_sql_table('clientes_usuarioaquitoy', ryf_conn)


# Prepare the data for merging

# Select the columns that are needed for the transformation
df_mensajeria_estado_servicio = df_mensajeria_estado_servicio[[
    'servicio_id', 'observaciones', 'fecha', 'hora'
]].rename(
    columns={
        'observaciones': 'estado_servicio_observaciones',
        'fecha': 'estado_servicio_fecha',
        'hora': 'estado_servicio_hora'
    }
)

# Select the columns that are needed for the transformation
df_clientes_usuario = df_clientes_usuario[[
    'id', 'sede_id'
]]

# Transform

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

# Drop the columns that are no longer needed
df_merged = df_merged.drop(columns=['id_y', 'servicio_id'])

# Rename the columns
df_merged = df_merged.rename(
    columns={
        'id_x': 'servicio_id'
    }
)

# Print columns
print(df_merged.columns)

# print count rows
print(df_merged.shape)


# Load (if needed)
df_merged.to_sql('trans_acu_servicios', etl_conn, if_exists='replace', index_label='key_trans_servicio')
