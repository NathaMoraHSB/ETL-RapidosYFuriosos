import pandas as pd
import helper

# Constants

TABLE_NAME = 'dim_novedad'
INDEX_NAME = 'key_dim_novedad'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()


def create_dim_novedad():
    # Extract

    df_tipo_novedad = pd.read_sql_table('mensajeria_tiponovedad', ryf_conn)

    # Transform

    df_tipo_novedad = df_tipo_novedad[[
        'id', 'nombre'
    ]].rename(
        columns={
            'id': 'tipo_novedad_id', 'nombre': 'tipo_novedad'
        }
    )

    # Merge

    # Perform the LEFT JOIN operations

    df_merge = df_tipo_novedad

    # Load

    helper.load_data("etl_conn", df_merge, TABLE_NAME, INDEX_NAME)
