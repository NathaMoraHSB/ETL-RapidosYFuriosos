import pandas as pd
import helper

# Constants

TABLE_NAME = 'dim_sede'
INDEX_NAME = 'key_dim_sede'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()


def create_dim_sede():
    # Extract

    df_sede = pd.read_sql_table('sede', ryf_conn)
    df_ciudad = pd.read_sql_table('ciudad', ryf_conn)

    # Transform

    df_sede = df_sede[['sede_id', 'nombre', 'direccion', 'telefono', 'nombre_contacto', 'ciudad_id']].rename(columns={'nombre': 'sede_nombre', 'direccion': 'sede_direccion', 'telefono': 'sede_telefono', 'nombre_contacto': 'contacto_sede'})
    df_ciudad = df_ciudad[['ciudad_id', 'nombre']].rename(columns={'nombre': 'ciudad_nombre'})

    # Merge

    # Perform the LEFT JOIN operations

    df_dim_sede = pd.merge(df_sede, df_ciudad, on='ciudad_id', how='left')

    # Clean

    # Drop columns that are not needed

    df_merged = df_dim_sede.drop(columns=['ciudad_id'])

    # Load

    helper.load_data(df_merged, TABLE_NAME, INDEX_NAME)
