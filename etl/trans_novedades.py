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
df_mensajeria_servicio = pd.read_sql_table('mensajeria_servicio', ryf_conn)
df_clientes_usuario = pd.read_sql_table('clientes_usuarioaquitoy', ryf_conn)
df_mensajeria_origen = pd.read_sql_table('mensajeria_origenservicio', ryf_conn)
df_mensajeria_destino = pd.read_sql_table('mensajeria_destinoservicio', ryf_conn)
df_ciudad = pd.read_sql_table('ciudad', ryf_conn)
df_mensajeria_tipo_servicio = pd.read_sql_table('mensajeria_tiposervicio', ryf_conn)

# Transform

df_mensajeria_novedad_servicio = df_mensajeria_novedad_servicio.rename(
    columns={
        'id': 'novedad_servicio_id',
        'descripcion': 'descripcion_novedad'
    }
)

df_mensajeria_servicio = df_mensajeria_servicio.rename(
    columns={
        'id': 'mensajeria_servicio_id',
        'descripcion': 'descripcion_servicio'
    }
)

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
df_merged = ((df_mensajeria_novedad_servicio.merge(
    df_mensajeria_servicio,
    left_on='servicio_id', right_on='mensajeria_servicio_id',
    how='left'
)).merge(
    df_clientes_usuario,
    left_on='usuario_id',
    right_on='id',
    how='left'
).merge(
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

# Drop columns
df_merged = df_merged.drop(columns=[
    'hora_visto_por_mensajero', 'visto_por_mensajero',
    'descripcion_multiples_origenes', 'tipo_pago_id', 'tipo_vehiculo_id', 'usuario_id',
    'hora_solicitud', 'fecha_deseada', 'mensajero2_id', 'mensajero3_id', 'origen_id', 'destino_id',
    'origen_ciudad_id', 'destino_ciudad_id', 'ciudad_origen_id', 'ciudad_destino_id', 'tipo_servicio_id',
    'mensajeria_servicio_id', 'servicio_id', 'id', 'mensajero_id_y'
]).rename(
    columns=
    {
        'mensajero_id_x': 'mensajero_id'
    }
)

# Fill empty strings

df_merged['descripcion_cancelado'] = df_merged['descripcion_cancelado'].replace("", "N/A")
df_merged['descripcion_cancelado'] = df_merged['descripcion_cancelado'].fillna("N/A")
df_merged['descripcion_novedad'] = df_merged['descripcion_novedad'].replace("", "N/A")
df_merged['descripcion_novedad'] = df_merged['descripcion_novedad'].fillna("N/A")
df_merged['descripcion_pago'] = df_merged['descripcion_pago'].replace("", "N/A")
df_merged['descripcion_pago'] = df_merged['descripcion_pago'].fillna("N/A")
df_merged['novedades'] = df_merged['novedades'].replace("", "N/A")
df_merged['novedades'] = df_merged['novedades'].fillna("N/A")



# Drop the existing table if it exists
helper.load_data("etl_conn", df_merged, TABLE_NAME, INDEX_NAME)
