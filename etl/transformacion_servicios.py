import pandas as pd
import helper


ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()

# Extract

df_mensajeria_servicio = pd.read_sql_table('mensajeria_servicio', ryf_conn)
df_mensajeria_estado_servicio = pd.read_sql_table('mensajeria_estadosservicio', ryf_conn)
df_clientes_usuario = pd.read_sql_table('clientes_usuarioaquitoy', ryf_conn)

# Transform
# SELECT
# ms.*,
# mes.observaciones AS estado_observaciones,
# mes.fecha AS estado_fecha,
# mes.hora AS estado_hora,
# mes.estado_id AS estado_id,
# cu.sede_id
# FROM
# public.mensajeria_servicio ms
# LEFT JOIN
# public.mensajeria_estadosservicio mes
# ON ms.id = mes.servicio_id
# LEFT JOIN
# public.clientes_usuarioaquitoy cu
# ON ms.usuario_id = cu.id
# order by id asc;

df_mensajeria_servicio = df_mensajeria_servicio.merge(df_mensajeria_estado_servicio, left_on='id', right_on='servicio_id', how='left')

df_mensajeria_servicio = df_mensajeria_servicio.merge(df_clientes_usuario, left_on='usuario_id', right_on='id', how='left')

print(df_mensajeria_servicio.head())

# Load

# df_mensajeria_servicio.to_sql('mensajeria_servicio', etl_conn, if_exists='replace', ndex_label='key_trans_servicio')


