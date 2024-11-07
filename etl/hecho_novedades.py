import pandas as pd
import helper


# Constants

TABLE_NAME = 'hecho_novedades'
INDEX_NAME = 'key_hecho_novedades'

# Establish connections

etl_conn = helper.get_etl_conn()


def create_hecho_novedades():
    # Extract

    # Load transformed data
    df_trans_novedades = pd.read_sql_table('trans_novedades', etl_conn).sort_values(
        by='key_trans_novedad',
        ascending=True)

    # Load dimensions

    df_dim_cliente = pd.read_sql_table('dim_cliente', etl_conn)
    df_dim_mensajero = pd.read_sql_table('dim_mensajero', etl_conn)
    df_dim_fecha = pd.read_sql_table('dim_fecha', etl_conn)
    df_dim_novedad = pd.read_sql_table('dim_novedad', etl_conn)
    # Transform

    # Merge with dim_cliente
    df_merged = pd.merge(
        df_trans_novedades,
        df_dim_cliente[['key_dim_cliente', 'cliente_id']],
        left_on='cliente_id',
        right_on='cliente_id').drop(columns=['cliente_id'])

    # Merge with dim_mensajero
    df_merged = pd.merge(
        df_merged,
        df_dim_mensajero[['key_dim_mensajero', 'mensajero_id']],
        left_on='mensajero_id',
        right_on='mensajero_id').drop(columns=['mensajero_id'])

    # Merge with dim_novedad
    df_merged = pd.merge(
        df_merged,
        df_dim_novedad[['key_dim_novedad', 'tipo_novedad_id']],
        left_on='tipo_novedad_id',
        right_on='tipo_novedad_id').drop(columns=['tipo_novedad_id'])

    # Merge with dim_fecha
    # fecha_novedad
    df_merged['fecha_novedad'] = pd.to_datetime(df_merged['fecha_novedad'])
    df_merged = pd.merge(
        df_merged,
        df_dim_fecha[['key_dim_fecha', 'fecha_id']],
        left_on=df_merged['fecha_novedad'].dt.strftime('%Y%m%d').astype(int),
        right_on='fecha_id').drop(columns=['fecha_id', 'fecha_novedad']).rename(columns={'key_dim_fecha': 'key_dim_fecha_novedad'})

    # Load

    helper.load_data(df_merged, TABLE_NAME, INDEX_NAME)

