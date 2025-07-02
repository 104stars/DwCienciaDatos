import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_sedes_oltp(engine_oltp):
    """
    Extrae información de sedes desde la base de datos OLTP.
    
    Args:
        engine_oltp (sqlalchemy.Engine): Motor de conexión al sistema OLTP
    
    Returns:
        pd.DataFrame: DataFrame con los datos de sedes extraídos
    """
    query = """
    SELECT
        sede_id AS "Sede_ID_Operacional",
        nombre AS "Nombre_Sede",
        direccion AS "Direccion_Sede",
        cliente_id,
        ciudad_id
    FROM
        public.sede
    """
    df = pd.read_sql(query, engine_oltp)
    print(f"Se extrajeron {len(df)} registros de sedes desde el OLTP.")
    return df

def transform_sedes(df_sedes, engine_dw):
    """
    Transforma los datos de sedes realizando lookup con las dimensiones Cliente y Geografía,
    y agregando una clave primaria surrogate.
    
    Args:
        df_sedes (pd.DataFrame): DataFrame con datos de sedes del OLTP
        engine_dw (sqlalchemy.Engine): Motor de conexión al Data Warehouse para lookup
    
    Returns:
        pd.DataFrame: DataFrame transformado con llaves foráneas y clave primaria surrogate
    """
    # Cargar dimensiones para realizar lookup de llaves foráneas
    df_dim_cliente = pd.read_sql('SELECT "Cliente_Key", "Cliente_ID_Operacional" FROM "Dim_Cliente"', engine_dw)
    df_dim_geografia = pd.read_sql('SELECT "Geografia_Key", "Ciudad_ID_Operacional" FROM "Dim_Geografia"', engine_dw)
    print("Dimensiones Cliente y Geografia cargadas desde el DW para lookup.")

    # Merge con dimensión Cliente para obtener Cliente_Key
    df_merged = pd.merge(
        df_sedes,
        df_dim_cliente,
        left_on='cliente_id',
        right_on='Cliente_ID_Operacional',
        how='left'
    )

    # Merge con dimensión Geografía para obtener Geografia_Key
    df_merged = pd.merge(
        df_merged,
        df_dim_geografia,
        left_on='ciudad_id',
        right_on='Ciudad_ID_Operacional',
        how='left'
    )
    print("Merge completado para buscar las llaves foráneas.")

    # Seleccionar columnas finales para la dimensión
    df_dim_sede = df_merged[[
        'Sede_ID_Operacional',
        'Nombre_Sede',
        'Cliente_Key',
        'Direccion_Sede',
        'Geografia_Key'
    ]]

    # Manejar valores nulos en Geografia_Key
    df_dim_sede.loc[:, 'Geografia_Key'] = df_dim_sede.loc[:, 'Geografia_Key'].fillna(-1).astype(int)

    # Agregar clave primaria surrogate
    df_dim_sede.insert(0, 'Sede_Key', range(1, 1 + len(df_dim_sede)))
    
    print("Transformación de sedes completada.")
    return df_dim_sede

def main():
    """
    Función principal que ejecuta el proceso ETL completo para la dimensión sede.
    Extrae datos del OLTP, los transforma con lookups y los carga en el Data Warehouse.
    """
    print("\nIniciando ETL para Dim_Sede...")
    
    engine_oltp = get_oltp_engine()
    engine_dw = get_dw_engine()

    # Extracción desde OLTP
    df_sedes_oltp = extract_sedes_oltp(engine_oltp)
    
    # Transformación con lookup de dimensiones
    df_dim_sede = transform_sedes(df_sedes_oltp, engine_dw)
    
    # Carga hacia DW
    load_df_to_dw(df_dim_sede, "Dim_Sede", engine_dw, "Sede_Key")
    
    print("Proceso de Dim_Sede completado.")

if __name__ == "__main__":
    main() 