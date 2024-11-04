import pandas as pd
import helper


ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()

# Extract

df_mensajeria_servicio = pd.read_sql_table('mensajeria_servicio', ryf_conn)
df_mensajeria_estado_servicio = pd.read_sql_table('mensajeria_estadoservicio', ryf_conn)



# Transform
# Save df_mensajeria_servicio join df_mensajeria_estado_servicio on id = id_servicio


