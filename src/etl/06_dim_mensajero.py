import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_mensajeros_oltp(engine_oltp):
    """
    Extrae información de mensajeros desde la base de datos OLTP, incluyendo
    el tipo de vehículo más frecuentemente utilizado por cada mensajero.
    
    Args:
        engine_oltp (sqlalchemy.Engine): Motor de conexión al sistema OLTP
    
    Returns:
        pd.DataFrame: DataFrame con los datos de mensajeros extraídos
    """
    query = """
    WITH VehiculoFrecuente AS (
        SELECT
            s.mensajero_id,
            s.tipo_vehiculo_id,
            ROW_NUMBER() OVER(PARTITION BY s.mensajero_id ORDER BY COUNT(*) DESC) as rn
        FROM
            public.mensajeria_servicio s
        WHERE
            s.mensajero_id IS NOT NULL AND s.tipo_vehiculo_id IS NOT NULL
        GROUP BY
            s.mensajero_id, s.tipo_vehiculo_id
    )
    SELECT
        m.id AS "Mensajero_ID_Operacional",
        (u.first_name || ' ' || u.last_name) AS "Nombre_Mensajero",
        tv.nombre as "Tipo_Vehiculo"
    FROM
        public.clientes_mensajeroaquitoy m
    JOIN
        public.auth_user u ON m.user_id = u.id
    LEFT JOIN
        (
            SELECT vf.mensajero_id, vf.tipo_vehiculo_id
            FROM VehiculoFrecuente vf
            WHERE vf.rn = 1
        ) AS vehiculo_principal ON m.id = vehiculo_principal.mensajero_id
    LEFT JOIN
        public.mensajeria_tipovehiculo tv ON vehiculo_principal.tipo_vehiculo_id = tv.id
    """
    df = pd.read_sql(query, engine_oltp)
    print(f"Se extrajeron {len(df)} registros de mensajeros desde el OLTP.")
    return df

def transform_mensajeros(df):
    """
    Transforma los datos de mensajeros manejando valores nulos en tipo de vehículo
    y agregando una clave primaria surrogate.
    
    Args:
        df (pd.DataFrame): DataFrame con datos de mensajeros del OLTP
    
    Returns:
        pd.DataFrame: DataFrame transformado con clave primaria surrogate
    """
    # Manejar valores nulos en tipo de vehículo
    df['Tipo_Vehiculo'] = df['Tipo_Vehiculo'].fillna('No asignado')

    # Agregar clave primaria surrogate
    df.insert(0, 'Mensajero_Key', range(1, 1 + len(df)))
    
    print("Transformación de mensajeros completada.")
    return df

def main():
    """
    Función principal que ejecuta el proceso ETL completo para la dimensión mensajero.
    Extrae datos del OLTP, los transforma y los carga en el Data Warehouse.
    """
    print("\nIniciando ETL para Dim_Mensajero...")
    
    engine_oltp = get_oltp_engine()
    engine_dw = get_dw_engine()

    # Extracción desde OLTP
    df_mensajeros_oltp = extract_mensajeros_oltp(engine_oltp)
    
    # Transformación
    df_dim_mensajero = transform_mensajeros(df_mensajeros_oltp)
    
    # Carga hacia DW
    load_df_to_dw(df_dim_mensajero, "Dim_Mensajero", engine_dw, "Mensajero_Key")
    
    print("Proceso de Dim_Mensajero completado.")

if __name__ == "__main__":
    main() 