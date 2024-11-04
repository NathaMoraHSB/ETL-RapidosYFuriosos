from sqlalchemy import create_engine
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

