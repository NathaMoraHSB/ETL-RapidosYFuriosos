import pandas as pd
import helper


# Constants

TABLE_NAME = 'hecho_servicios_hora'
INDEX_NAME = 'key_hecho_servicios_hora'

# Establish connections

etl_conn = helper.get_etl_conn()


def create_hecho_servicios_hora():
    # Extract

    # Load transformed data
    df_trans_servicios = pd.read_sql_table('trans_servicios', etl_conn).sort_values(
        by='key_trans_servicio',
        ascending=True)

    # Load dimensions
    df_dim_hora = pd.read_sql_table('dim_hora', etl_conn)

    # Transform

    # Merge with dim_hora
    # hora_solicitud
    df_trans_servicios['hora_solicitud'] = df_trans_servicios['hora_solicitud'].apply(lambda x: x.strftime('%H') if pd.notnull(x) else None)
    df_merged = pd.merge(
        df_trans_servicios,
        df_dim_hora[['key_dim_hora', 'hora']],
        left_on='hora_solicitud',
        right_on='hora').drop(columns=['hora', 'hora_solicitud']).rename(columns={'key_dim_hora': 'key_dim_hora_solicitud'})

    df_merged = df_merged.groupby('key_dim_hora_solicitud').size().reset_index(name='numero_servicios')

    # Load

    helper.load_data(df_merged, TABLE_NAME, INDEX_NAME)
