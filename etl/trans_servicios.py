import pandas as pd
import helper


# Constants

TABLE_NAME = 'trans_servicios'
INDEX_NAME = 'key_trans_servicio'

# Establish connections

ryf_conn = helper.get_ryf_conn()
etl_conn = helper.get_etl_conn()

# Extract

df_mensajeria_servicio = pd.read_sql_table('mensajeria_servicio', ryf_conn)
df_clientes_usuario = pd.read_sql_table('clientes_usuarioaquitoy', ryf_conn)
df_mensajeria_estado_servicio = pd.read_sql_table('mensajeria_estadosservicio', ryf_conn)

# Transform

df_mensajeria_servicio = df_mensajeria_servicio[[
    'id', 'cliente_id', 'fecha_solicitud', 'hora_solicitud', 'usuario_id', 'mensajero_id', 'mensajero2_id', 'mensajero3_id'
]].rename(
    columns={
        'id': 'servicio_id'
    }
).sort_values(by='servicio_id', ascending=True)


df_clientes_usuario = df_clientes_usuario[[
    'id', 'sede_id'
]].rename(
    columns={
        'id': 'clientes_usuario_id'
    }
)


df_mensajeria_estado_servicio = df_mensajeria_estado_servicio[[
    'servicio_id', 'hora', 'fecha', 'estado_id'
]].rename(
    columns={
        'hora': 'estado_hora',
        'fecha': 'estado_fecha',
        'estado_id': 'estado_servicio_id'
    }
)

# Merge

# Perform the LEFT JOIN operations
df_merged = df_mensajeria_servicio.merge(
    df_clientes_usuario,
    how='left',
    left_on='usuario_id',
    right_on='clientes_usuario_id'
)

# Clean


# Update mensajero_id based on mensajero2_id and mensajero3_id
def update_mensajero_ids(row):
    # If mensajero2_id is not null, use its value
    if not pd.isnull(row['mensajero2_id']):
        row['mensajero_id'] = row['mensajero2_id']
    # If mensajero3_id is not null and mensajero_id is still null, use its value
    if not pd.isnull(row['mensajero3_id']):
        row['mensajero_id'] = row['mensajero3_id']
    return row


# Update mensajero_id based on mensajero2_id and mensajero3_id
df_merged = df_merged.apply(update_mensajero_ids, axis=1)

# Drop rows where mensajero_id is still None (these are the rows marked for dropping)
df_merged = df_merged.dropna(subset=['mensajero_id'])

# Drop unnecessary columns
df_merged = df_merged.drop(columns=[
    'clientes_usuario_id', 'usuario_id', 'mensajero2_id', 'mensajero3_id'
])


# Process service statuses
def process_service_statuses(row):

    # Find all statuses for the given servicio_id
    df_service_statuses = df_mensajeria_estado_servicio[df_mensajeria_estado_servicio['servicio_id'] == row['servicio_id']]

    print(f"Processing servicio_id: {row['servicio_id']}")

    # If there are no statuses, return a Series of None values
    if df_service_statuses.empty:
        return pd.Series([None]*16, index=[
            'estado_fecha_asignacion', 'estado_hora_asignacion', 'tiempo_minutos_asignacion', 'tiempo_horas_asignacion',
            'estado_fecha_recogida', 'estado_hora_recogida', 'tiempo_minutos_recogida', 'tiempo_horas_recogida',
            'estado_fecha_entrega', 'estado_hora_entrega', 'tiempo_minutos_entrega', 'tiempo_horas_entrega',
            'estado_fecha_cerrado', 'estado_hora_cerrado', 'tiempo_minutos_cerrado', 'tiempo_horas_cerrado'
        ])

    # Order statuses by estado_servicio_id, estado_fecha, and estado_hora
    df_service_statuses = df_service_statuses.sort_values(by=['estado_servicio_id', 'estado_fecha', 'estado_hora'])

    # Helper to get a status by ID, defaulting to None if not found
    def get_status(state_id, get_latest=False):
        state = df_service_statuses[df_service_statuses['estado_servicio_id'] == state_id]
        if state.empty:
            return None, None
        return (state.iloc[-1] if get_latest else state.iloc[0])[['estado_fecha', 'estado_hora']]

    # Get each status date and time
    requested_date, requested_time = get_status(1)
    assigned_date, assigned_time = get_status(2, get_latest=True)
    picked_up_date, picked_up_time = get_status(4)
    delivered_date, delivered_time = get_status(5)
    closed_date, closed_time = get_status(6)
    # If the service is not closed, set closed_date and closed_time to delivered_date and delivered_time
    if closed_date is None:
        closed_date, closed_time = delivered_date, delivered_time

    # Check for congruency: requested <= assigned <= picked_up <= delivered <= closed
    def is_congruent(*args):
        # Check if any of the datetime fields are None
        for i in range(0, len(args)-3, 2):
            if None in args[i:i+4]:
                return False  # If any field is None, return False
            if pd.to_datetime(f"{args[i]} {args[i+1]}") > pd.to_datetime(f"{args[i+2]} {args[i+3]}"):
                return False  # If any date/time comparison fails, return False
        return True

    # If the statuses are not congruent, return a Series of None values
    if not is_congruent(requested_date, requested_time, assigned_date, assigned_time,
                        assigned_date, assigned_time, picked_up_date, picked_up_time,
                        picked_up_date, picked_up_time, delivered_date, delivered_time,
                        delivered_date, delivered_time, closed_date, closed_time):
        return pd.Series([None]*16, index=[
            'estado_fecha_asignacion', 'estado_hora_asignacion', 'tiempo_minutos_asignacion', 'tiempo_horas_asignacion',
            'estado_fecha_recogida', 'estado_hora_recogida', 'tiempo_minutos_recogida', 'tiempo_horas_recogida',
            'estado_fecha_entrega', 'estado_hora_entrega', 'tiempo_minutos_entrega', 'tiempo_horas_entrega',
            'estado_fecha_cerrado', 'estado_hora_cerrado', 'tiempo_minutos_cerrado', 'tiempo_horas_cerrado'
        ])

    # Calculate durations in minutes and hours between states
    def calc_duration(start_date, start_time, end_date, end_time):
        if None in [start_date, start_time, end_date, end_time]:
            return None, None
        start = pd.to_datetime(f"{start_date} {start_time}")
        end = pd.to_datetime(f"{end_date} {end_time}")
        total_minutes = (end - start).total_seconds() / 60  # convert to minutes
        total_hours = total_minutes / 60  # convert to hours
        return round(total_minutes, 2), round(total_hours, 2)

    # Calculate durations
    requested_to_assigned_minutes, requested_to_assigned_hours = calc_duration(requested_date, requested_time, assigned_date, assigned_time)
    assigned_to_picked_up_minutes, assigned_to_picked_up_hours = calc_duration(assigned_date, assigned_time, picked_up_date, picked_up_time)
    picked_up_to_delivered_minutes, picked_up_to_delivered_hours = calc_duration(picked_up_date, picked_up_time, delivered_date, delivered_time)
    delivered_to_closed_minutes, delivered_to_closed_hours = calc_duration(delivered_date, delivered_time, closed_date, closed_time)

    # Return the final Series
    return pd.Series([
        assigned_date, assigned_time, requested_to_assigned_minutes, requested_to_assigned_hours,
        picked_up_date, picked_up_time, assigned_to_picked_up_minutes, assigned_to_picked_up_hours,
        delivered_date, delivered_time, picked_up_to_delivered_minutes, picked_up_to_delivered_hours,
        closed_date, closed_time, delivered_to_closed_minutes, delivered_to_closed_hours
    ], index=[
        'estado_fecha_asignacion', 'estado_hora_asignacion', 'tiempo_minutos_asignacion', 'tiempo_horas_asignacion',
        'estado_fecha_recogida', 'estado_hora_recogida', 'tiempo_minutos_recogida', 'tiempo_horas_recogida',
        'estado_fecha_entrega', 'estado_hora_entrega', 'tiempo_minutos_entrega', 'tiempo_horas_entrega',
        'estado_fecha_cerrado', 'estado_hora_cerrado', 'tiempo_minutos_cerrado', 'tiempo_horas_cerrado'
    ])


# Apply the function to the dataframe and concatenate the resulting Series
df_merged = df_merged.apply(lambda row: pd.concat([row, process_service_statuses(row)]), axis=1)

# Remove rows that have none values in datetime fields
df_merged = df_merged.dropna(subset=[
    'estado_fecha_asignacion', 'estado_hora_asignacion', 'tiempo_minutos_asignacion', 'tiempo_horas_asignacion',
    'estado_fecha_recogida', 'estado_hora_recogida', 'tiempo_minutos_recogida', 'tiempo_horas_recogida',
    'estado_fecha_entrega', 'estado_hora_entrega', 'tiempo_minutos_entrega', 'tiempo_horas_entrega',
    'estado_fecha_cerrado', 'estado_hora_cerrado', 'tiempo_minutos_cerrado', 'tiempo_horas_cerrado'
])
# Reset the index
df_merged = df_merged.reset_index(drop=True)

# Load
helper.load_data("etl_conn", df_merged, TABLE_NAME, INDEX_NAME)
