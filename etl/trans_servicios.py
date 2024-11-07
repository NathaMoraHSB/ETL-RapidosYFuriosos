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

# Process service statuses for each service saving:
# estado_fecha_asignacion
# estado_hora_asignacion
# estado_fecha_recogida
# estado_hora_recogida
# estado_fecha_entrega
# estado_hora_entrega
# estado_fecha_cerrado
# estado_hora_cerrado
# And calculate the duration of each status from df_mensajeria_estado_servicio
# tiempo_minutos_asignacion
# tiempo_horas_asignacion
# tiempo_minutos_recogida
# tiempo_horas_recogida
# tiempo_minutos_entrega
# tiempo_horas_entrega
# tiempo_minutos_cerrado
# tiempo_horas_cerrado

# using the following rules:

def process_service_statuses(row):
    # Find all statuses for the given servicio_id
    df_service_statuses = df_mensajeria_estado_servicio[df_mensajeria_estado_servicio['servicio_id'] == row['servicio_id']]
    print("df_service_statuses:")
    print(df_service_statuses)
    if df_service_statuses.empty:
        return pd.Series([None]*14, index=[
            'estado_fecha_asignacion', 'estado_hora_asignacion', 'tiempo_minutos_asignacion',
            'estado_fecha_recogida', 'estado_hora_recogida', 'tiempo_minutos_recogida',
            'estado_fecha_entrega', 'estado_hora_entrega', 'tiempo_minutos_entrega',
            'estado_fecha_cerrado', 'estado_hora_cerrado', 'tiempo_minutos_cerrado'
        ])

    # Order statuses by estado_servicio_id, estado_fecha, and estado_hora
    df_service_statuses = df_service_statuses.sort_values(by=['estado_servicio_id', 'estado_fecha', 'estado_hora'])

    # Helper to get a status by ID, defaulting to None if not found
    def get_status(state_id, get_latest=False):
        state = df_service_statuses[df_service_statuses['estado_servicio_id'] == state_id]
        if state.empty:
            return None, None
        return (state.iloc[-1] if get_latest else state.iloc[0])[['estado_fecha', 'estado_hora']]

    # Get each relevant status
    requested_date, requested_time = get_status(1)
    assigned_date, assigned_time = get_status(2, get_latest=True)
    picked_up_date, picked_up_time = get_status(4)
    delivered_date, delivered_time = get_status(5)
    closed_date, closed_time = get_status(6)
    if closed_date is None:
        closed_date, closed_time = delivered_date, delivered_time

    # Calculate durations in minutes between states
    def calc_duration(start_date, start_time, end_date, end_time):
        if None in [start_date, start_time, end_date, end_time]:
            return None
        start = pd.to_datetime(f"{start_date} {start_time}")
        print("start:", start)
        end = pd.to_datetime(f"{end_date} {end_time}")
        print("end:", end)
        return int((end - start).total_seconds() / 60)  # convert to minutes

    # Debug each calculation to verify values
    print("requested_date:", requested_date, "requested_time:", requested_time)
    print("assigned_date:", assigned_date, "assigned_time:", assigned_time)
    print("picked_up_date:", picked_up_date, "picked_up_time:", picked_up_time)
    print("delivered_date:", delivered_date, "delivered_time:", delivered_time)
    print("closed_date:", closed_date, "closed_time:", closed_time)

    print("Duration from requested to assigned:", calc_duration(requested_date, requested_time, assigned_date, assigned_time))
    print("Duration from assigned to picked up:", calc_duration(assigned_date, assigned_time, picked_up_date, picked_up_time))
    print("Duration from picked up to delivered:", calc_duration(picked_up_date, picked_up_time, delivered_date, delivered_time))
    print("Duration from delivered to closed:", calc_duration(delivered_date, delivered_time, closed_date, closed_time))

    # exit()  # Use exit to stop for debugging and check printed results

    # If calculations are correct, return the final Series
    return pd.Series([
        assigned_date, assigned_time, calc_duration(requested_date, requested_time, assigned_date, assigned_time),
        picked_up_date, picked_up_time, calc_duration(assigned_date, assigned_time, picked_up_date, picked_up_time),
        delivered_date, delivered_time, calc_duration(picked_up_date, picked_up_time, delivered_date, delivered_time),
        closed_date, closed_time, calc_duration(delivered_date, delivered_time, closed_date, closed_time)
    ], index=[
        'estado_fecha_asignacion', 'estado_hora_asignacion', 'tiempo_minutos_asignacion',
        'estado_fecha_recogida', 'estado_hora_recogida', 'tiempo_minutos_recogida',
        'estado_fecha_entrega', 'estado_hora_entrega', 'tiempo_minutos_entrega',
        'estado_fecha_cerrado', 'estado_hora_cerrado', 'tiempo_minutos_cerrado'
    ])

test  = process_service_statuses(df_merged.iloc[0])
print("\ntest:")
print(test)


# Apply the function to the dataframe
# df_merged = df_merged.apply(process_service_statuses, axis=1)






# # Clean
#
# # Update mensajero_id based on mensajero2_id and mensajero3_id
# df_merged = df_merged.apply(helper.update_mensajero_ids, axis=1)
#
# # Drop rows where mensajero_id is still None (these are the rows marked for dropping)
# df_merged = df_merged.dropna(subset=['mensajero_id'])
#
# # Drop unnecessary columns
# df_merged = df_merged.drop(columns=[
#     'clientes_usuario_id', 'usuario_id', 'mensajero2_id', 'mensajero3_id'
# ])
#
# # Drop the existing table if it exists
# helper.load_data("etl_conn", df_merged, TABLE_NAME, INDEX_NAME)
