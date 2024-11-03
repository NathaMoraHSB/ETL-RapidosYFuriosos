import pandas as pd
import helper


ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()


# Query para extraer la información de la dimensión sede
query = """
SELECT
    s.sede_id,
    s.nombre AS sede_nombre,
    s.direccion AS sede_direccion,
    s.telefono AS sede_telefono,
    s.nombre_contacto AS contacto_sede,
    c.nombre AS ciudad_nombre,
    cl.nombre AS cliente_nombre
FROM
    public.sede s
LEFT JOIN
    public.ciudad c ON s.ciudad_id = c.ciudad_id
LEFT JOIN
    public.cliente cl ON s.cliente_id = cl.cliente_id;
"""

# Leer los datos desde la base de datos 'RAPIDOS-Y_FURIOSOS' a un DataFrame de Pandas
df_sede = pd.read_sql(query, ryf_conn)

df_sede.to_sql('dim_sede', etl_conn, index=False, if_exists='replace')