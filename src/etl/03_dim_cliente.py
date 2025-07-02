import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_clientes_oltp(engine_oltp):
    """
    Extrae información de clientes desde la base de datos OLTP.
    
    Args:
        engine_oltp (sqlalchemy.Engine): Motor de conexión al sistema OLTP
    
    Returns:
        pd.DataFrame: DataFrame con los datos de clientes extraídos
    """
    query = """
    SELECT
        cliente_id AS "Cliente_ID_Operacional",
        nombre AS "Nombre_Cliente",
        sector AS "Industria_Cliente"
    FROM
        public.cliente
    """
    df = pd.read_sql(query, engine_oltp)
    print(f"Se extrajeron {len(df)} registros de clientes desde el OLTP.")
    return df

def transform_clientes(df):
    """
    Transforma los datos de clientes agregando una clave primaria surrogate.
    
    Args:
        df (pd.DataFrame): DataFrame con datos de clientes del OLTP
    
    Returns:
        pd.DataFrame: DataFrame transformado con clave primaria surrogate
    """
    # Agregar clave primaria surrogate
    df.insert(0, 'Cliente_Key', range(1, 1 + len(df)))
    
    print("Transformación de clientes completada (generación de Key).")
    return df

def main():
    """
    Función principal que ejecuta el proceso ETL completo para la dimensión cliente.
    Extrae datos del OLTP, los transforma y los carga en el Data Warehouse.
    """
    print("Iniciando ETL para Dim_Cliente...")
    
    try:
        engine_oltp = get_oltp_engine()
        engine_dw = get_dw_engine()
        print("Conexiones a OLTP y DW establecidas.")
    except Exception as e:
        print(f"Error al conectar a las bases de datos: {e}")
        return

    # Extracción desde OLTP
    df_clientes_oltp = extract_clientes_oltp(engine_oltp)

    # Transformación
    df_dim_cliente = transform_clientes(df_clientes_oltp)
    
    # Carga hacia DW
    load_df_to_dw(df_dim_cliente, "Dim_Cliente", engine_dw, "Cliente_Key")
    
    print("Proceso de Dim_Cliente completado.")

if __name__ == "__main__":
    main() 