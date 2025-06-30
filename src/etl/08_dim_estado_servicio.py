import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_estados_oltp(engine_oltp):
    """
    Extrae los datos de los estados de servicio desde la base de datos operacional.
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
    Aplica las transformaciones necesarias a los datos de los estados.
    """
    # En este caso, la transformación principal se hace en el SQL (renombrar y ordenar).
    # Solo necesitamos generar la clave subrogada.
    
    # Generar la clave subrogada
    df.insert(0, 'Estado_Servicio_Key', range(1, 1 + len(df)))
    
    print("Transformación de estados de servicio completada.")
    return df

def main():
    """
    Orquesta el proceso de ETL para la dimensión de estados de servicio.
    """
    print("\nIniciando ETL para Dim_Estado_Servicio...")
    
    engine_oltp = get_oltp_engine()
    engine_dw = get_dw_engine()

    df_oltp = extract_estados_oltp(engine_oltp)
    df_dim = transform_estados(df_oltp)
    
    load_df_to_dw(df_dim, "Dim_Estado_Servicio", engine_dw, "Estado_Servicio_Key")
    
    print("Proceso de Dim_Estado_Servicio completado.")

if __name__ == "__main__":
    main() 