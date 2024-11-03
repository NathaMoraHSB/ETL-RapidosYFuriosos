import pandas as pd
import helper


ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()


# Query para extraer la información de la dimensión mensajero
query = """
SELECT
    cm.id AS mensajero_id,
    cm.user_id,
    cm.activo,
    cm.fecha_entrada,
    cm.fecha_salida,
    cm.telefono,
    cm.url_foto,
    c.nombre AS ciudad_nombre,
    au.username,
    au.first_name,
    au.last_name,
    au.email
FROM
    public.clientes_mensajeroaquitoy cm
LEFT JOIN
    public.ciudad c ON cm.ciudad_operacion_id = c.ciudad_id
LEFT JOIN
    public.auth_user au ON cm.user_id = au.id;
"""

# Leer los datos desde la base de datos 'RAPIDOS-Y_FURIOSOS' a un DataFrame de Pandas
df_mensajero = pd.read_sql(query, ryf_conn)

# Cargar los datos a la base de datos 'ETL_RYF' como la dimensión 'dim_mensajero'
df_mensajero.to_sql('dim_mensajero', etl_conn, index=False, if_exists='replace')

