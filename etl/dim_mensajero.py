import pandas as pd
import helper

# Constants

TABLE_NAME = 'dim_mensajero'
INDEX_NAME = 'key_dim_mensajero'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()


def create_dim_mensajero():
    # Extract

    df_clientes_mensajero = pd.read_sql_table('clientes_mensajeroaquitoy', ryf_conn)
    df_ciudad = pd.read_sql_table('ciudad', ryf_conn)
    df_auth_user = pd.read_sql_table('auth_user', ryf_conn)

    # Transform

    df_clientes_mensajero = df_clientes_mensajero[['id', 'user_id', 'activo', 'fecha_entrada', 'fecha_salida', 'telefono', 'url_foto', 'ciudad_operacion_id']].rename(columns={'id': 'mensajero_id'})
    df_ciudad = df_ciudad[['ciudad_id', 'nombre']].rename(columns={'nombre': 'ciudad_nombre'})
    df_auth_user = df_auth_user[['id', 'username', 'first_name', 'last_name', 'email']]

    # Merge

    # Perform the LEFT JOIN operations

    df_dim_mensajero = pd.merge(df_clientes_mensajero, df_ciudad, left_on='ciudad_operacion_id', right_on='ciudad_id', how='left')
    df_merged = pd.merge(df_dim_mensajero, df_auth_user, left_on='user_id', right_on='id', how='left')

    # Clean

    # Drop the columns that are not needed (fecha_entrada y fecha_salida)
    df_merged = df_merged.drop(columns=['ciudad_id', 'ciudad_operacion_id', 'id', 'fecha_entrada', 'fecha_salida'])

    # Drop the user_id column
    df_merged = df_merged.drop(columns=['user_id'])

    # Fill empty strings
    df_merged['ciudad_nombre'] = df_merged['ciudad_nombre'].fillna("N/A")

    # Load

    helper.load_data(df_merged, TABLE_NAME, INDEX_NAME)
