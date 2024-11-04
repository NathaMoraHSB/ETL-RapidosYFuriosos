import pandas as pd
import helper

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()


df_cliente = pd.read_sql('SELECT * FROM public.cliente', ryf_conn)

df_ciudad = pd.read_sql('SELECT ciudad_id, nombre AS ciudad, departamento_id FROM public.ciudad', ryf_conn)

df_departamento = pd.read_sql('SELECT departamento_id, nombre AS departamento FROM public.departamento', ryf_conn)

df_tipo_cliente = pd.read_sql('SELECT tipo_cliente_id, nombre AS tipo_cliente FROM public.tipo_cliente', ryf_conn)



df_cliente_ciudad = pd.merge(df_cliente, df_ciudad, on='ciudad_id', how='left')


df_cliente_ciudad_departamento = pd.merge(df_cliente_ciudad, df_departamento, on='departamento_id', how='left')

df_dim_cliente = pd.merge(df_cliente_ciudad_departamento, df_tipo_cliente, on='tipo_cliente_id', how='left')


df_dim_cliente = df_dim_cliente[[
    'cliente_id', 'nit_cliente', 'nombre', 'email', 'direccion', 'telefono',
    'nombre_contacto', 'ciudad', 'departamento', 'tipo_cliente', 'activo', 'sector'
]]



df_dim_cliente.to_sql(
    'dim_cliente',  # Nombre de la tabla destino en la base de datos.
    con=etl_conn,   # Conexión a la base de datos ETL.
    if_exists='replace',  # Reemplaza la tabla si ya existe.
    index_label='key_dim_cliente'  # Nombre de la columna de índice (clave primaria).
)
