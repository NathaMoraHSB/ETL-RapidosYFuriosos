import pandas as pd
import helper

# Constants

TABLE_NAME = 'dim_cliente'
INDEX_NAME = 'key_dim_cliente'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()


def create_dim_cliente():

    # Extract

    df_cliente = pd.read_sql_table('cliente', ryf_conn)
    df_ciudad = pd.read_sql_table('ciudad', ryf_conn)
    df_departamento = pd.read_sql_table('departamento', ryf_conn)
    df_tipo_cliente = pd.read_sql_table('tipo_cliente', ryf_conn)


    # Transform

    df_ciudad = df_ciudad[['ciudad_id', 'nombre', 'departamento_id']].rename(columns={'nombre': 'ciudad'})
    df_departamento = df_departamento[['departamento_id', 'nombre']].rename(columns={'nombre': 'departamento'})
    df_tipo_cliente = df_tipo_cliente[['tipo_cliente_id', 'nombre']].rename(columns={'nombre': 'tipo_cliente'})


    # Perform the LEFT JOIN operations

    df_cliente_ciudad = pd.merge(df_cliente, df_ciudad, on='ciudad_id', how='left')
    df_cliente_ciudad_departamento = pd.merge(df_cliente_ciudad, df_departamento, on='departamento_id', how='left')
    df_dim_cliente = pd.merge(df_cliente_ciudad_departamento, df_tipo_cliente, on='tipo_cliente_id', how='left')


    df_merged = df_dim_cliente[[
        'cliente_id', 'nit_cliente', 'nombre', 'email', 'direccion', 'telefono',
        'nombre_contacto', 'ciudad', 'departamento', 'tipo_cliente', 'activo', 'sector'
    ]]

    # Load
    helper.load_data(df_merged, TABLE_NAME, INDEX_NAME)
