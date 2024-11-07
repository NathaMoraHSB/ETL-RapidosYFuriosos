import pandas as pd
from datetime import date
import holidays
import helper

# Constants

TABLE_NAME = 'dim_fecha'
INDEX_NAME = 'key_dim_fecha'


def create_dim_fecha():
    # Crear un rango de fechas por minuto para obtener horas y minutos también
    dates = pd.date_range(start='1/1/2023', end='12/31/2025 23:59', freq='D')

    # Inicializar el DataFrame
    dim_fecha = pd.DataFrame({
        "fecha_completa": dates,  # La fecha completa en formato YYYY-MM-DD HH:MM
    })

    # Añadir columnas como el año, mes, día, día de la semana y el trimestre del año.

    dim_fecha["fecha_id"] = dim_fecha["fecha_completa"].dt.strftime('%Y%m%d').astype(int)  # ID de fecha en formato YYYYMMDD
    dim_fecha["año"] = dim_fecha["fecha_completa"].dt.year
    dim_fecha["mes"] = dim_fecha["fecha_completa"].dt.month
    dim_fecha["nombre_mes"] = dim_fecha["fecha_completa"].dt.month_name()
    dim_fecha["semana"] = dim_fecha["fecha_completa"].dt.isocalendar().week
    dim_fecha["dia"] = dim_fecha["fecha_completa"].dt.day
    dim_fecha["nombre_dia"] = dim_fecha["fecha_completa"].dt.day_name()
    dim_fecha["trimestre"] = dim_fecha["fecha_completa"].dt.quarter

    co_holidays = holidays.CO(language="es")  # Usar librería holidays para identificar feriados en Colombia

    # Aplicar las reglas para identificar si es feriado usando la columna correcta
    dim_fecha["is_Holiday"] = dim_fecha["fecha_completa"].dt.date.apply(lambda x: x in co_holidays)
    dim_fecha["holiday"] = dim_fecha["fecha_completa"].dt.date.apply(lambda x: co_holidays.get(x))

    # Añadir fecha de guardado
    dim_fecha["saved"] = date.today()

    # Identificar si es fin de semana (sábado o domingo)
    dim_fecha["weekend"] = dim_fecha["fecha_completa"].dt.weekday >= 5


    # Añadir indicadores booleanos para días laborales y fines de semana
    dim_fecha["es_dia_laboral"] = dim_fecha["fecha_completa"].dt.weekday < 5  # True si es de lunes a viernes
    dim_fecha["es_fin_de_semana"] = dim_fecha["fecha_completa"].dt.weekday >= 5  # True si es sábado o domingo

    # Marcar feriados en Colombia
    co_holidays = holidays.CO(years=dim_fecha["año"].unique(), language="es")
    dim_fecha["es_feriado"] = dim_fecha["fecha_completa"].dt.date.isin(co_holidays)

    # Load
    helper.load_data(dim_fecha, TABLE_NAME, INDEX_NAME)
