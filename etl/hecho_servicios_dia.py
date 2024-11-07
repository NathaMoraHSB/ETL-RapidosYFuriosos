import pandas as pd
import helper


# Constants

TABLE_NAME = 'hecho_servicios_dia'
INDEX_NAME = 'key_hecho_servicios_dia'

# Establish connections

etl_conn = helper.get_etl_conn()


# Extract

df_trans_servicios = pd.read_sql_table('trans_servicios', etl_conn)

# Transform

df_merged = df_trans_servicios

# Load

helper.load_data("etl_conn", df_merged, TABLE_NAME, INDEX_NAME)
