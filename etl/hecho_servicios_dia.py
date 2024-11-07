import pandas as pd
import helper


# Constants

TABLE_NAME = 'hecho_servicios_dia'
INDEX_NAME = 'key_hecho_servicios_dia'

# Establish connections

etl_conn = helper.get_etl_conn()


def create_hecho_servicios_dia():
    # Extract

    # Load transformed data
    df_trans_servicios = pd.read_sql_table('trans_servicios', etl_conn).sort_values(
        by='key_trans_servicio',
        ascending=True)

    # Load dimensions

    df_dim_fecha = pd.read_sql_table('dim_fecha', etl_conn)

    # Transform

    # Merge with dim_fecha

    df_trans_servicios['fecha_solicitud'] = pd.to_datetime(df_trans_servicios['fecha_solicitud'])
    df_merged = pd.merge(
        df_trans_servicios,
        df_dim_fecha[['key_dim_fecha', 'fecha_id']],
        left_on=df_trans_servicios['fecha_solicitud'].dt.strftime('%Y%m%d').astype(int),
        right_on='fecha_id').drop(columns=['fecha_id', 'fecha_solicitud']).rename(columns={'key_dim_fecha': 'key_dim_fecha_solicitud'})

    df_merged = df_merged.groupby('key_dim_fecha_solicitud').size().reset_index(name='numero_servicios')


    # Load
    helper.load_data(df_merged, TABLE_NAME, INDEX_NAME)
