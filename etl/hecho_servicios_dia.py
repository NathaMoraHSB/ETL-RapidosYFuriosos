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

    # Load dimensions

    df_dim_cliente = pd.read_sql_table('dim_cliente', etl_conn)
    df_dim_mensajero = pd.read_sql_table('dim_mensajero', etl_conn)
    df_dim_sede = pd.read_sql_table('dim_sede', etl_conn)
    df_dim_fecha = pd.read_sql_table('dim_fecha', etl_conn)

    # Transform
    # Merge with dim_cliente
    df_merged = pd.merge(
        df_trans_servicios,
        df_dim_cliente[['key_dim_cliente', 'cliente_id']],
        left_on='cliente_id',
        right_on='cliente_id').drop(columns=['cliente_id'])

    # Merge with dim_mensajero
    df_merged = pd.merge(
        df_merged,
        df_dim_mensajero[['key_dim_mensajero', 'mensajero_id']],
        left_on='mensajero_id',
        right_on='mensajero_id').drop(columns=['mensajero_id'])

    # Merge with dim_sede
    df_merged = pd.merge(
        df_merged,
        df_dim_sede[['key_dim_sede', 'sede_id']],
        left_on='sede_id',
        right_on='sede_id').drop(columns=['sede_id'])

    # Merge with dim_fecha for all dates
    # fecha_solicitud
    df_merged['fecha_solicitud'] = pd.to_datetime(df_merged['fecha_solicitud'])
    df_merged = pd.merge(
        df_merged,
        df_dim_fecha[['key_dim_fecha', 'fecha_id']],
        left_on=df_merged['fecha_solicitud'].dt.strftime('%Y%m%d').astype(int),
        right_on='fecha_id').drop(columns=['fecha_id', 'fecha_solicitud']).rename(columns={'key_dim_fecha': 'key_dim_fecha_solicitud'})

    df_merged = df_merged.groupby(['key_dim_fecha_solicitud', 'key_dim_mensajero', 'key_dim_cliente', 'key_dim_sede']).size().reset_index(name='numero_servicios')

    # Load
    helper.load_data(df_merged, TABLE_NAME, INDEX_NAME)
