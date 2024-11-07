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

def load_data(df, table_name, index_name):
    # Determine the correct database connection
    db_conn = get_etl_conn()

    # Use the connection object to execute SQL statements
    with db_conn.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS public.{table_name} CASCADE;"))
        conn.commit()

    # Load the DataFrame into the SQL table
    facts_tables = [
        "hecho_servicios_hora",
        "hecho_acumulating_servicios",
        "hecho_novedades",
        "hecho_servicios_dia"
    ]

    if table_name in facts_tables:
        df.to_sql(table_name, db_conn, if_exists='replace', index_label=index_name)

        #add constraint to the table on index_name primary key and unique
        with db_conn.connect() as conn:
            conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({index_name});"))
            conn.commit()

        #Get column names
        columns = df.columns

        #Get colums that stat with key_dim and make reference to the dimension tables
        dim_columns = [col for col in columns if col.startswith("key_dim")]
        for dim_column in dim_columns:
            #Get the dimension table name
            dim_table = dim_column.split("_")[2]
            #Add foreign key constraint
            with db_conn.connect() as conn:
                conn.execute(text(f"ALTER TABLE {table_name} ADD FOREIGN KEY ({dim_column}) REFERENCES dim_{dim_table}(key_dim_{dim_table});"))
                conn.commit()

    else:
        df.to_sql(table_name, db_conn, if_exists='replace', index_label=index_name)
        # add constraint to the table on index_name primary key and unique
        with db_conn.connect() as conn:
            conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({index_name});"))
            conn.commit()

