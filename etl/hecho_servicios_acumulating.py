import pandas as pd
import helper


# Constants

TABLE_NAME = 'hecho_acumulating_servicios'
INDEX_NAME = 'key_hecho_acumulating_servicio'

# Establish connections

etl_conn = helper.get_etl_conn()


def create_hecho_servicios_acumulating():

    # Extract

    # Load transformed data
    df_trans_servicios = pd.read_sql_table('trans_servicios', etl_conn).sort_values(
        by='key_trans_servicio',
        ascending=True)

    # Load dimensions

    df_dim_cliente = pd.read_sql_table('dim_cliente', etl_conn)
    df_dim_mensajero = pd.read_sql_table('dim_mensajero', etl_conn)
    df_dim_sede = pd.read_sql_table('dim_sede', etl_conn)
    df_dim_fecha = pd.read_sql_table('dim_fecha', etl_conn)
    df_dim_hora = pd.read_sql_table('dim_hora', etl_conn)

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

    # estado_fecha_asignacion
    df_merged['estado_fecha_asignacion'] = pd.to_datetime(df_merged['estado_fecha_asignacion'])
    df_merged = pd.merge(
        df_merged,
        df_dim_fecha[['key_dim_fecha', 'fecha_id']],
        left_on=df_merged['estado_fecha_asignacion'].dt.strftime('%Y%m%d').astype(int),
        right_on='fecha_id').drop(columns=['fecha_id', 'estado_fecha_asignacion']).rename(columns={'key_dim_fecha': 'key_dim_fecha_asignacion'})

    # estado_fecha_recogida
    df_merged['estado_fecha_recogida'] = pd.to_datetime(df_merged['estado_fecha_recogida'])
    df_merged = pd.merge(
        df_merged,
        df_dim_fecha[['key_dim_fecha', 'fecha_id']],
        left_on=df_merged['estado_fecha_recogida'].dt.strftime('%Y%m%d').astype(int),
        right_on='fecha_id').drop(columns=['fecha_id', 'estado_fecha_recogida']).rename(columns={'key_dim_fecha': 'key_dim_fecha_recogida'})

    # estado_fecha_entrega
    df_merged['estado_fecha_entrega'] = pd.to_datetime(df_merged['estado_fecha_entrega'])
    df_merged = pd.merge(
        df_merged,
        df_dim_fecha[['key_dim_fecha', 'fecha_id']],
        left_on=df_merged['estado_fecha_entrega'].dt.strftime('%Y%m%d').astype(int),
        right_on='fecha_id').drop(columns=['fecha_id', 'estado_fecha_entrega']).rename(columns={'key_dim_fecha': 'key_dim_fecha_entrega'})

    # estado_fecha_cerrado
    df_merged['estado_fecha_cerrado'] = pd.to_datetime(df_merged['estado_fecha_cerrado'])
    df_merged = pd.merge(
        df_merged,
        df_dim_fecha[['key_dim_fecha', 'fecha_id']],
        left_on=df_merged['estado_fecha_cerrado'].dt.strftime('%Y%m%d').astype(int),
        right_on='fecha_id').drop(columns=['fecha_id', 'estado_fecha_cerrado']).rename(columns={'key_dim_fecha': 'key_dim_fecha_cerrado'})

    # Merge with dim_hora for all hours
    # hora_solicitud
    df_merged['hora_solicitud'] = df_merged['hora_solicitud'].apply(lambda x: x.strftime('%H') if pd.notnull(x) else None)
    df_merged = pd.merge(
        df_merged,
        df_dim_hora[['key_dim_hora', 'hora']],
        left_on='hora_solicitud',
        right_on='hora').drop(columns=['hora', 'hora_solicitud']).rename(columns={'key_dim_hora': 'key_dim_hora_solicitud'})

    # estado_hora_asignacion
    df_merged['estado_hora_asignacion'] = df_merged['estado_hora_asignacion'].apply(lambda x: x.strftime('%H') if pd.notnull(x) else None)
    df_merged = pd.merge(
        df_merged,
        df_dim_hora[['key_dim_hora', 'hora']],
        left_on='estado_hora_asignacion',
        right_on='hora').drop(columns=['hora', 'estado_hora_asignacion']).rename(columns={'key_dim_hora': 'key_dim_hora_asignacion'})

    # estado_hora_recogida
    df_merged['estado_hora_recogida'] = df_merged['estado_hora_recogida'].apply(lambda x: x.strftime('%H') if pd.notnull(x) else None)
    df_merged = pd.merge(
        df_merged,
        df_dim_hora[['key_dim_hora', 'hora']],
        left_on='estado_hora_recogida',
        right_on='hora').drop(columns=['hora', 'estado_hora_recogida']).rename(columns={'key_dim_hora': 'key_dim_hora_recogida'})

    # estado_hora_entrega
    df_merged['estado_hora_entrega'] = df_merged['estado_hora_entrega'].apply(lambda x: x.strftime('%H') if pd.notnull(x) else None)
    df_merged = pd.merge(
        df_merged,
        df_dim_hora[['key_dim_hora', 'hora']],
        left_on='estado_hora_entrega',
        right_on='hora').drop(columns=['hora', 'estado_hora_entrega']).rename(columns={'key_dim_hora': 'key_dim_hora_entrega'})

    # estado_hora_cerrado
    df_merged['estado_hora_cerrado'] = df_merged['estado_hora_cerrado'].apply(lambda x: x.strftime('%H') if pd.notnull(x) else None)
    df_merged = pd.merge(
        df_merged,
        df_dim_hora[['key_dim_hora', 'hora']],
        left_on='estado_hora_cerrado',
        right_on='hora').drop(columns=['hora', 'estado_hora_cerrado']).rename(columns={'key_dim_hora': 'key_dim_hora_cerrado'})

    # Load
    helper.load_data(df_merged, TABLE_NAME, INDEX_NAME)
