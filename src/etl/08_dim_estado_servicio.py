import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_estados_oltp(engine_oltp):
    """
    Extrae información de estados de servicio desde la base de datos OLTP,
    ordenados según el flujo secuencial del proceso.
    
    Args:
        engine_oltp (sqlalchemy.Engine): Motor de conexión al sistema OLTP
    
    Returns:
        pd.DataFrame: DataFrame con los datos de estados extraídos y ordenados
    """
    query = """
    SELECT
        id AS "Orden_Estado",
        nombre AS "Nombre_Estado"
    FROM
        public.mensajeria_estado
    ORDER BY
        id
    """
    df = pd.read_sql(query, engine_oltp)
    print(f"Se extrajeron {len(df)} registros de estados de servicio desde el OLTP.")
    return df

def transform_estados(df):
    """
    Transforma los datos de estados de servicio agregando una clave primaria surrogate.
    Mantiene el orden secuencial original de los estados.
    
    Args:
        df (pd.DataFrame): DataFrame con datos de estados del OLTP
    
    Returns:
        pd.DataFrame: DataFrame transformado con clave primaria surrogate
    """
    # Agregar clave primaria surrogate
    df.insert(0, 'Estado_Servicio_Key', range(1, 1 + len(df)))
    
    print("Transformación de estados de servicio completada.")
    return df

def main():
    """
    Función principal que ejecuta el proceso ETL completo para la dimensión estado de servicio.
    Extrae datos del OLTP, los transforma y los carga en el Data Warehouse.
    """
    print("\nIniciando ETL para Dim_Estado_Servicio...")
    
    engine_oltp = get_oltp_engine()
    engine_dw = get_dw_engine()

    # Extracción desde OLTP
    df_oltp = extract_estados_oltp(engine_oltp)
    
    # Transformación
    df_dim = transform_estados(df_oltp)
    
    # Carga hacia DW
    load_df_to_dw(df_dim, "Dim_Estado_Servicio", engine_dw, "Estado_Servicio_Key")
    
    print("Proceso de Dim_Estado_Servicio completado.")

if __name__ == "__main__":
    main() 