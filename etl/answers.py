import pandas as pd
import helper

# Establish connections

etl_conn = helper.get_etl_conn()

# Extract

# Load Fact Tables
df_hecho_novedades = pd.read_sql_table('hecho_novedades', etl_conn)
df_hecho_acumulating_servicios = pd.read_sql_table('hecho_acumulating_servicios', etl_conn)
df_hecho_servicios_dia = pd.read_sql_table('hecho_servicios_dia', etl_conn)
df_hecho_servicios_hora = pd.read_sql_table('hecho_servicios_hora', etl_conn)

# Load dimensions

df_dim_cliente = pd.read_sql_table('dim_cliente', etl_conn)
df_dim_fecha = pd.read_sql_table('dim_fecha', etl_conn)
df_dim_hora = pd.read_sql_table('dim_hora', etl_conn)
df_dim_mensajero = pd.read_sql_table('dim_mensajero', etl_conn)
df_dim_novedad = pd.read_sql_table('dim_novedad', etl_conn)
df_dim_sede = pd.read_sql_table('dim_sede', etl_conn)


# Count number of registers of fact tables
print('Fact Tables')
print('Hecho Novedades:', df_hecho_novedades.shape[0])
print('Hecho Acumulating Servicios:', df_hecho_acumulating_servicios.shape[0])
print('Hecho Servicios Dia:', df_hecho_servicios_dia.shape[0])
print('Hecho Servicios Hora:', df_hecho_servicios_hora.shape[0], '\n')

# 1) En qué meses del año los clientes solicitan más servicios de mensajería

print('Meses con más solicitudes de servicios de mensajería')
response_1 = df_hecho_servicios_dia.merge(
    df_dim_fecha,
    left_on='key_dim_fecha_solicitud',
    right_on='key_dim_fecha')[['nombre_mes', 'mes', 'numero_servicios']]
response_1 = response_1.groupby(['nombre_mes', 'mes']).sum().sort_values(by='numero_servicios', ascending=False)
print(response_1, '\n')

# 2) Cuáles son los días donde más solicitudes hay

print('Días con más solicitudes de servicios de mensajería')
response_2 = df_hecho_servicios_dia.merge(
    df_dim_fecha,
    left_on='key_dim_fecha_solicitud',
    right_on='key_dim_fecha')[['numero_servicios','nombre_dia']]
response_2 = response_2.groupby(['nombre_dia']).sum().sort_values(by='numero_servicios', ascending=False)
print(response_2, '\n')


# 3) A qué hora los mensajeros están más ocupados.

print('Horas con más solicitudes de servicios de mensajería')
response_3 = df_hecho_servicios_hora.merge(
    df_dim_hora,
    left_on='key_dim_hora_solicitud',
    right_on='key_dim_hora')[['hora', 'numero_servicios']]
response_3 = response_3.groupby(['hora']).sum().sort_values(by='numero_servicios', ascending=False)
print(response_3, '\n')

# 4) Número de servicios solicitados por cliente y por mes

print('Número de servicios solicitados por cliente y por mes')
response_4 = df_hecho_acumulating_servicios.merge(
    df_dim_cliente,
    left_on='key_dim_cliente',
    right_on='key_dim_cliente').merge(
    df_dim_fecha,
    left_on='key_dim_fecha_solicitud',
    right_on='key_dim_fecha')

response_4 = response_4.groupby(['cliente_id', 'nombre', 'nombre_mes']).size().reset_index(name='numero_servicios')
print(response_4, '\n')

# 5) Mensajeros más eficientes (Los que más servicios prestan)

print('Mensajeros más eficientes')
response_5 = df_hecho_acumulating_servicios.merge(
    df_dim_mensajero,
    left_on='key_dim_mensajero',
    right_on='key_dim_mensajero').groupby(['mensajero_id', 'username']).size().reset_index(name='numero_servicios')
response_5 = response_5.sort_values(by='numero_servicios', ascending=False)
print(response_5, '\n')


# 6) Cuáles son las sedes que más servicios solicitan por cada cliente.

print('Sedes que más servicios solicitan por cada cliente')
response_6 = df_hecho_acumulating_servicios.merge(
    df_dim_sede,
    left_on='key_dim_sede',
    right_on='key_dim_sede').merge(
    df_dim_cliente,
    left_on='key_dim_cliente',
    right_on='key_dim_cliente').groupby(['cliente_id', 'nombre', 'sede_id', 'sede_nombre']).size().reset_index(name='numero_servicios')
response_6 = response_6.sort_values(by=['cliente_id', 'numero_servicios'], ascending=False)
print(response_6, '\n')

# 7) Cuál es el tiempo promedio de entrega desde que se solicita el servicio hasta que se cierra el caso.

print('Tiempo promedio de entrega \n')
# make average of total_tiempo_minutos and total_tiempo_horas
response_7 = df_hecho_acumulating_servicios
avg_tiempo_minutos = round(response_7['total_tiempo_minutos'].mean(), 2)
avg_tiempo_horas = round(response_7['total_tiempo_horas'].mean(), 2)
print('Tiempo promedio en minutos:', avg_tiempo_minutos)
print('Tiempo promedio en horas:', avg_tiempo_horas, '\n')

# 8) Mostrar los tiempos de espera por cada fase del servicio: Iniciado, Con mensajero asignado,
# recogido en origen, Entregado en Destino, Cerrado. En que fase del servicio hay más demoras?

print('Tiempo de espera promedio por cada fase del servicio')
response_8 = df_hecho_acumulating_servicios

def get_avg_time(df, column):
    return round(df[column].mean(), 2)

response_8_mins = {
    'Asignacion minutos': get_avg_time(response_8, 'tiempo_minutos_asignacion'),
    'Recogido minutos': get_avg_time(response_8, 'tiempo_minutos_recogida'),
    'Entregado minutos': get_avg_time(response_8, 'tiempo_minutos_entrega'),
    'Cerrado minutos': get_avg_time(response_8, 'tiempo_minutos_cerrado'),
}

response_8_hours = {
    'Asignacion horas': get_avg_time(response_8, 'tiempo_horas_asignacion'),
    'Recogido horas': get_avg_time(response_8, 'tiempo_horas_recogida'),
    'Entregado horas': get_avg_time(response_8, 'tiempo_horas_entrega'),
    'Cerrado horas': get_avg_time(response_8, 'tiempo_horas_cerrado')
}

for key, value in response_8_mins.items():
    print(key, value)
for key, value in response_8_hours.items():
    print(key, value)
# get the max value
max_value_mins = max(response_8_mins, key=response_8_mins.get)
max_value_hours = max(response_8_hours, key=response_8_hours.get)
print('Fase con más demoras en minutos:', max_value_mins, " Tiempo: ", response_8_mins[max_value_mins])
print('Fase con más demoras en horas:', max_value_hours, " Tiempo: ", response_8_hours[max_value_hours])

print('\n')

# 9) Cuáles son las novedades que más se presentan durante la prestación del servicio?

print('Novedades que más se presentan durante la prestación del servicio')
response_9 = df_hecho_novedades.merge(
    df_dim_novedad,
    left_on='key_dim_novedad',
    right_on='key_dim_novedad').groupby(['tipo_novedad']).size().reset_index(name='numero_novedades')
response_9 = response_9.sort_values(by='numero_novedades', ascending=False)
print(response_9, '\n')
