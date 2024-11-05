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
df_mensajeria_origen = pd.read_sql_table('mensajeria_origenservicio', ryf_conn)
df_mensajeria_destino = pd.read_sql_table('mensajeria_destinoservicio', ryf_conn)
df_ciudad = pd.read_sql_table('ciudad', ryf_conn)
df_mensajeria_tipo_servicio = pd.read_sql_table('mensajeria_tiposervicio', ryf_conn)

# Transform

df_clientes_usuario = df_clientes_usuario[[
    'id', 'sede_id'
]]

df_mensajeria_tipo_servicio = df_mensajeria_tipo_servicio[[
    'id', 'nombre']].rename(
    columns={
        'id': 'tipo_servicio_id',
        'nombre': 'tipo_servicio'
    }
)

df_ciudad = df_ciudad[[
    'ciudad_id', 'nombre'
]]


df_mensajeria_origen = df_mensajeria_origen[[
    'id', 'direccion', 'ciudad_id']].rename(
    columns={
        'id': 'origen_id',
        'direccion': 'origen_servicio_direccion',
        'ciudad_id': 'origen_ciudad_id'
    }
)

df_mensajeria_destino = df_mensajeria_destino[[
    'id', 'direccion', 'ciudad_id']].rename(
    columns={
        'id': 'destino_id',
        'direccion': 'destino_servicio_direccion',
        'ciudad_id': 'destino_ciudad_id'
    }
)

# Merge


# Perform the LEFT JOIN operations

df_mensajeria_origen = pd.merge(
    df_mensajeria_origen, df_ciudad,
    left_on='origen_ciudad_id', right_on='ciudad_id',
    how='left'
).drop(columns=['ciudad_id']).rename(columns={'nombre': 'origen_ciudad_nombre'})

df_mensajeria_destino = pd.merge(
    df_mensajeria_destino, df_ciudad,
    left_on='destino_ciudad_id', right_on='ciudad_id',
    how='left'
).drop(columns=['ciudad_id']).rename(columns={'nombre': 'destino_ciudad_nombre'})


# Perform the LEFT JOIN operations
df_merged = ((df_mensajeria_servicio.merge(
    df_clientes_usuario,
    left_on='usuario_id',
    right_on='id',
    how='left'
)).merge(
    df_mensajeria_tipo_servicio,
    left_on='tipo_servicio_id',
    right_on='tipo_servicio_id',
    how='left'
).merge(
    df_mensajeria_origen,
    left_on='origen_id',
    right_on='origen_id',
    how='left'
).merge(
    df_mensajeria_destino,
    left_on='destino_id',
    right_on='destino_id',
    how='left'
))

# Clean

# Update mensajero_id based on mensajero2_id and mensajero3_id
df_merged = df_merged.apply(helper.update_mensajero_ids, axis=1)

# Drop rows where mensajero_id is still None (these are the rows marked for dropping)
df_merged = df_merged.dropna(subset=['mensajero_id'])

# Drop columns
df_merged = df_merged.drop(columns=[
    'id_y', 'hora_visto_por_mensajero', 'visto_por_mensajero',
    'descripcion_multiples_origenes', 'tipo_pago_id', 'tipo_vehiculo_id', 'usuario_id',
    'hora_solicitud', 'fecha_deseada', 'mensajero2_id', 'mensajero3_id', 'origen_id', 'destino_id',
    'origen_ciudad_id', 'destino_ciudad_id', 'ciudad_origen_id', 'ciudad_destino_id', 'tipo_servicio_id'
])

# Fill empty strings

df_merged['descripcion_cancelado'] = df_merged['descripcion_cancelado'].replace("", "N/A")
df_merged['descripcion_cancelado'] = df_merged['descripcion_cancelado'].fillna("N/A")
df_merged['descripcion_pago'] = df_merged['descripcion_pago'].replace("", "N/A")
df_merged['descripcion_pago'] = df_merged['descripcion_pago'].fillna("N/A")
df_merged['novedades'] = df_merged['novedades'].replace("", "N/A")
df_merged['novedades'] = df_merged['novedades'].fillna("N/A")

# Rename the columns
df_merged = df_merged.rename(
    columns={
        'id_x': 'servicio_id'
    }
)

# Drop the existing table if it exists
helper.load_data("etl_conn", df_merged, TABLE_NAME, INDEX_NAME)
