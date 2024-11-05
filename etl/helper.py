import pandas as pd
from sqlalchemy import create_engine, text
import yaml

# Cargar configuración de conexión desde el archivo YAML
with open('../config.yml', 'r') as f:
    config = yaml.safe_load(f)
    config_ryf = config['RAPIDOS-Y_FURIOSOS']
    config_etl = config['ETL_RYF']

# Construir la URL de conexión para cada base de datos
url_ryf = (f"{config_ryf['drivername']}://{config_ryf['user']}:{config_ryf['password']}@"
           f"{config_ryf['host']}:{config_ryf['port']}/{config_ryf['dbname']}")

url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@"
           f"{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}")

def get_ryf_conn():
    return create_engine(url_ryf)

def get_etl_conn():
    return create_engine(url_etl)

def load_data(connection, df, table_name, index_name):
    # Determine the correct database connection
    if connection == "etl_conn":
        db_conn = get_etl_conn()
    else:
        raise ValueError("Invalid connection name.")

    # Use the connection object to execute SQL statements
    with db_conn.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))

    # Load the DataFrame into the SQL table
    df.to_sql(table_name, db_conn, if_exists='replace', index_label=index_name)

def update_mensajero_ids(row):
    # If mensajero2_id is not null, use its value
    if not pd.isnull(row['mensajero2_id']):
        row['mensajero_id'] = row['mensajero2_id']
    # If mensajero3_id is not null and mensajero_id is still null, use its value
    if not pd.isnull(row['mensajero3_id']):
        row['mensajero_id'] = row['mensajero3_id']
    return row

