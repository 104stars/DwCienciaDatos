import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_sedes_oltp(engine_oltp):
    """
    Extrae los datos de las sedes desde la base de datos operacional.
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
    Transforma los datos de las sedes, buscando las claves foráneas en el DW.
    """
    # 1. Leer las dimensiones necesarias del DW para usarlas como tablas de búsqueda (lookup)
    df_dim_cliente = pd.read_sql('SELECT "Cliente_Key", "Cliente_ID_Operacional" FROM "Dim_Cliente"', engine_dw)
    df_dim_geografia = pd.read_sql('SELECT "Geografia_Key", "Ciudad_ID_Operacional" FROM "Dim_Geografia"', engine_dw)
    print("Dimensiones Cliente y Geografia cargadas desde el DW para lookup.")

    # 2. Unir (merge) con la dimensión de cliente para obtener la Cliente_Key
    df_merged = pd.merge(
        df_sedes,
        df_dim_cliente,
        left_on='cliente_id',
        right_on='Cliente_ID_Operacional',
        how='left'
    )

    # 3. Unir (merge) con la dimensión de geografía para obtener la Geografia_Key
    df_merged = pd.merge(
        df_merged,
        df_dim_geografia,
        left_on='ciudad_id',
        right_on='Ciudad_ID_Operacional',
        how='left'
    )
    print("Merge completado para buscar las llaves foráneas.")

    # 4. Seleccionar y renombrar las columnas finales para la dimensión
    df_dim_sede = df_merged[[
        'Sede_ID_Operacional',
        'Nombre_Sede',
        'Cliente_Key',
        'Direccion_Sede',
        'Geografia_Key'
    ]]

    # Manejar el caso de que una sede no encuentre una geografía (aunque no debería pasar)
    df_dim_sede.loc[:, 'Geografia_Key'] = df_dim_sede.loc[:, 'Geografia_Key'].fillna(-1).astype(int)

    # 5. Generar la clave subrogada
    df_dim_sede.insert(0, 'Sede_Key', range(1, 1 + len(df_dim_sede)))
    
    print("Transformación de sedes completada.")
    return df_dim_sede

def main():
    """
    Orquesta el proceso de ETL para la dimensión de sedes.
    """
    print("\nIniciando ETL para Dim_Sede...")
    
    engine_oltp = get_oltp_engine()
    engine_dw = get_dw_engine()

    df_sedes_oltp = extract_sedes_oltp(engine_oltp)
    df_dim_sede = transform_sedes(df_sedes_oltp, engine_dw)
    
    load_df_to_dw(df_dim_sede, "Dim_Sede", engine_dw, "Sede_Key")
    
    print("Proceso de Dim_Sede completado.")

if __name__ == "__main__":
    main() 