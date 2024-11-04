import pandas as pd
import helper


ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()

query = """
SELECT
    mn.id AS novedad_id,
    mn.descripcion AS novedad_descripcion,
    mn.fecha_novedad,
    TO_CHAR(mn.fecha_novedad, 'YYYY-MM-DD') AS novedad_fecha,
    TO_CHAR(mn.fecha_novedad, 'HH24:MI:SS') AS novedad_hora,
    mt.nombre AS tipo_novedad_nombre
FROM
    public.mensajeria_novedadesservicio mn
LEFT JOIN
    public.mensajeria_tiponovedad mt ON mn.tipo_novedad_id = mt.id;
"""

# Leer los datos desde la base de datos 'RAPIDOS-Y_FURIOSOS' a un DataFrame de Pandas
df_novedad = pd.read_sql(query, ryf_conn)


# Verificar duplicados y datos nulos
df_novedad.drop_duplicates(subset=['novedad_id'], inplace=True)
df_novedad.fillna({'novedad_descripcion': 'Sin descripción'}, inplace=True)

df_novedad.to_sql('dim_novedad', etl_conn, if_exists='replace', index=False)