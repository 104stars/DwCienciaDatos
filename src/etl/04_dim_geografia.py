import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_geografia_oltp(engine_oltp):
    """
    Extrae información geográfica (ciudades y departamentos) desde la base de datos OLTP.
    
    Args:
        engine_oltp (sqlalchemy.Engine): Motor de conexión al sistema OLTP
    
    Returns:
        pd.DataFrame: DataFrame con los datos geográficos extraídos
    """
    query = """
    SELECT
        c.ciudad_id AS "Ciudad_ID_Operacional",
        c.nombre AS "Ciudad",
        d.nombre AS "Departamento"
    FROM
        public.ciudad c
    JOIN
        public.departamento d ON c.departamento_id = d.departamento_id
    """
    df = pd.read_sql(query, engine_oltp)
    print(f"Se extrajeron {len(df)} registros de geografía desde el OLTP.")
    return df

def transform_geografia(df):
    """
    Transforma los datos geográficos agregando información de país y clave primaria surrogate.
    
    Args:
        df (pd.DataFrame): DataFrame con datos geográficos del OLTP
    
    Returns:
        pd.DataFrame: DataFrame transformado con país y clave primaria surrogate
    """
    # Agregar información de país (por defecto Colombia)
    df['Pais'] = 'Colombia'

    # Agregar clave primaria surrogate
    df.insert(0, 'Geografia_Key', range(1, 1 + len(df)))
    
    print("Transformación de geografía completada.")
    return df

def main():
    """
    Función principal que ejecuta el proceso ETL completo para la dimensión geografía.
    Extrae datos del OLTP, los transforma y los carga en el Data Warehouse.
    """
    print("\nIniciando ETL para Dim_Geografia...")
    
    try:
        engine_oltp = get_oltp_engine()
        engine_dw = get_dw_engine()
        print("Conexiones a OLTP y DW establecidas.")
    except Exception as e:
        print(f"Error al conectar a las bases de datos: {e}")
        return

    # Extracción desde OLTP
    df_geografia_oltp = extract_geografia_oltp(engine_oltp)

    # Transformación
    df_dim_geografia = transform_geografia(df_geografia_oltp)
    
    # Carga hacia DW
    load_df_to_dw(df_dim_geografia, "Dim_Geografia", engine_dw, "Geografia_Key")
    
    print("Proceso de Dim_Geografia completado.")

if __name__ == "__main__":
    main() 