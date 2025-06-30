import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine

def extract_cambios_estado_oltp(engine_oltp):
    """
    Extrae los eventos de cambio de estado, uniéndolos con la información
    del servicio para obtener el contexto necesario.
    """
    query = """
    SELECT
        es.id AS "Servicio_Estado_ID",
        es.servicio_id AS "Servicio_ID_Operacional",
        es.estado_id,
        es.fecha,
        es.hora,
        s.cliente_id,
        s.mensajero_id,
        s.tipo_servicio_id,
        uaq.sede_id AS "Sede_Origen_ID",
        (SELECT d.ciudad_id FROM public.mensajeria_destinoservicio d WHERE d.id = s.destino_id) AS "Geografia_Destino_ID",
        (SELECT d.direccion FROM public.mensajeria_destinoservicio d WHERE d.id = s.destino_id) AS "Direccion_Destino",
        (SELECT mn.tipo_novedad_id FROM public.mensajeria_novedadesservicio mn WHERE mn.servicio_id = s.id ORDER BY mn.fecha_novedad DESC LIMIT 1) AS "Tipo_Novedad_ID"
    FROM
        public.mensajeria_estadosservicio es
    JOIN
        public.mensajeria_servicio s ON es.servicio_id = s.id
    LEFT JOIN
        public.clientes_usuarioaquitoy uaq ON s.usuario_id = uaq.id
    """
    df = pd.read_sql(query, engine_oltp)
    print(f"Se extrajeron {len(df)} eventos de cambio de estado desde el OLTP.")
    return df

def transform_fact_table(df_oltp, engine_dw):
    """
    Transforma los datos extraídos en la tabla de hechos, buscando todas las claves foráneas.
    """
    # 1. Cargar todas las dimensiones del DW en memoria
    df_dim_fecha = pd.read_sql('SELECT "Fecha_Key", "Fecha_Completa" FROM "Dim_Fecha"', engine_dw)
    df_dim_hora = pd.read_sql('SELECT "Hora_Key", "Hora_Completa" FROM "Dim_Hora"', engine_dw)
    df_dim_cliente = pd.read_sql('SELECT "Cliente_Key", "Cliente_ID_Operacional" FROM "Dim_Cliente"', engine_dw)
    df_dim_sede = pd.read_sql('SELECT "Sede_Key", "Sede_ID_Operacional" FROM "Dim_Sede"', engine_dw)
    df_dim_geografia = pd.read_sql('SELECT "Geografia_Key", "Ciudad_ID_Operacional" FROM "Dim_Geografia"', engine_dw)
    df_dim_mensajero = pd.read_sql('SELECT "Mensajero_Key", "Mensajero_ID_Operacional" FROM "Dim_Mensajero"', engine_dw)
    df_dim_estado = pd.read_sql('SELECT "Estado_Servicio_Key", "Orden_Estado" FROM "Dim_Estado_Servicio"', engine_dw)
    df_dim_urgencia = pd.read_sql('SELECT "Urgencia_Servicio_Key", "Urgencia_ID_Operacional" FROM "Dim_Urgencia_Servicio"', engine_dw)
    df_dim_novedad = pd.read_sql('SELECT "Novedad_Key", "Descripcion_Novedad", "Novedad_ID_Operacional" FROM "Dim_Novedad"', engine_dw)
    print("Dimensiones cargadas desde el DW para lookup.")

    # Convertir columnas de fecha/hora a tipos adecuados para el merge
    df_oltp['fecha'] = pd.to_datetime(df_oltp['fecha']).dt.date
    df_dim_fecha['Fecha_Completa'] = pd.to_datetime(df_dim_fecha['Fecha_Completa']).dt.date
    df_oltp['hora'] = pd.to_datetime(df_oltp['hora'].astype(str), errors='coerce').dt.time
    df_dim_hora['Hora_Completa'] = pd.to_datetime(df_dim_hora['Hora_Completa'].astype(str), errors='coerce').dt.time
    
    # 2. Realizar los merges (lookups) para encontrar las claves
    df_merged = df_oltp
    df_merged = pd.merge(df_merged, df_dim_fecha, left_on='fecha', right_on='Fecha_Completa', how='left')
    df_merged = pd.merge(df_merged, df_dim_hora, left_on='hora', right_on='Hora_Completa', how='left')
    df_merged = pd.merge(df_merged, df_dim_cliente, left_on='cliente_id', right_on='Cliente_ID_Operacional', how='left')
    df_merged = pd.merge(df_merged, df_dim_sede, left_on='Sede_Origen_ID', right_on='Sede_ID_Operacional', how='left')
    df_merged = pd.merge(df_merged, df_dim_geografia, left_on='Geografia_Destino_ID', right_on='Ciudad_ID_Operacional', how='left')
    df_merged = pd.merge(df_merged, df_dim_mensajero, left_on='mensajero_id', right_on='Mensajero_ID_Operacional', how='left')
    df_merged = pd.merge(df_merged, df_dim_estado, left_on='estado_id', right_on='Orden_Estado', how='left')
    df_merged = pd.merge(df_merged, df_dim_urgencia, left_on='tipo_servicio_id', right_on='Urgencia_ID_Operacional', how='left')
    
    # Renombrar claves para reflejar contexto
    df_merged = df_merged.rename(columns={
        'Sede_Key': 'Sede_Origen_Key',
        'Geografia_Key': 'Geografia_Destino_Key'
    })
    
    # Lookup de Novedad es especial
    df_merged = pd.merge(df_merged, df_dim_novedad.add_prefix('novedad_'), left_on='Tipo_Novedad_ID', right_on='novedad_Novedad_ID_Operacional', how='left')
    novedad_sin_key = df_dim_novedad[df_dim_novedad['Descripcion_Novedad'] == 'Sin Novedad']['Novedad_Key'].iloc[0]
    df_merged['Novedad_Key'] = df_merged['novedad_Novedad_Key'].fillna(novedad_sin_key)

    # 3. Ensamblar la tabla de hechos final
    df_fact = df_merged[[
        'Fecha_Key', 'Hora_Key', 'Cliente_Key', 'Sede_Origen_Key', 'Geografia_Destino_Key',
        'Mensajero_Key', 'Estado_Servicio_Key', 'Urgencia_Servicio_Key', 'Novedad_Key',
        'Servicio_ID_Operacional', 'Direccion_Destino'
    ]]
    
    # Añadir timestamp y contador
    df_fact['Timestamp_Estado'] = pd.to_datetime(
        df_merged['fecha'].astype(str) + ' ' + df_merged['hora'].astype(str),
        errors='coerce'
    )
    df_fact['Contador_Estados'] = 1
    
    # Generar la clave primaria
    df_fact.insert(0, 'Servicio_Estado_Key', range(1, 1 + len(df_fact)))

    # Rellenar claves foráneas nulas (ej. mensajero no asignado) con un valor estándar (-1)
    for col in ['Mensajero_Key', 'Urgencia_Servicio_Key']:
        df_fact[col] = df_fact[col].fillna(-1)

    print("Transformación de la tabla de hechos completada.")
    return df_fact.astype({'Novedad_Key': 'int64', 'Mensajero_Key': 'int64', 'Urgencia_Servicio_Key': 'int64'})
    
def load_fact_table_to_dw(df, engine_dw):
    """
    Carga la tabla de hechos en el DW.
    """
    df.to_sql("Fact_Cambio_Estado_Servicio", engine_dw, if_exists='replace', index=False, chunksize=10000)
    print("Tabla de hechos cargada exitosamente en el DW.")

def main():
    """
    Orquesta el proceso de ETL para la tabla de hechos.
    """
    print("\nIniciando ETL para Fact_Cambio_Estado_Servicio...")
    
    engine_oltp = get_oltp_engine()
    engine_dw = get_dw_engine()

    df_oltp = extract_cambios_estado_oltp(engine_oltp)
    df_fact = transform_fact_table(df_oltp, engine_dw)
    
    load_fact_table_to_dw(df_fact, engine_dw)
    
    print("Proceso de Fact_Cambio_Estado_Servicio completado.")

if __name__ == "__main__":
    main() 