import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_novedades_oltp(engine_oltp):
    """
    Extrae información de tipos de novedad desde la base de datos OLTP.
    
    Args:
        engine_oltp (sqlalchemy.Engine): Motor de conexión al sistema OLTP
    
    Returns:
        pd.DataFrame: DataFrame con los datos de tipos de novedad extraídos
    """
    query = """
    SELECT
        id AS "Novedad_ID_Operacional",
        nombre AS "Descripcion_Novedad"
    FROM
        public.mensajeria_tiponovedad
    """
    df = pd.read_sql(query, engine_oltp)
    print(f"Se extrajeron {len(df)} registros de tipos de novedad desde el OLTP.")
    return df

def transform_novedades(df):
    """
    Transforma los datos de novedad agregando categorización general,
    un registro especial para "Sin Novedad" y una clave primaria surrogate.
    
    Args:
        df (pd.DataFrame): DataFrame con datos de tipos de novedad del OLTP
    
    Returns:
        pd.DataFrame: DataFrame transformado con registro especial y clave primaria surrogate
    """
    # Agregar categoría general para todas las novedades
    df['Categoria_Novedad'] = 'General'
    
    # Seleccionar columnas finales
    df = df[['Novedad_ID_Operacional', 'Descripcion_Novedad', 'Categoria_Novedad']]
    
    # Agregar registro especial para servicios sin novedad
    sin_novedad = pd.DataFrame([{
        'Novedad_ID_Operacional': -1,
        'Descripcion_Novedad': 'Sin Novedad',
        'Categoria_Novedad': 'Ninguna'
    }])
    df = pd.concat([sin_novedad, df], ignore_index=True)

    # Agregar clave primaria surrogate
    df.insert(0, 'Novedad_Key', range(1, 1 + len(df)))
    
    print("Transformación de novedades completada.")
    return df

def main():
    """
    Función principal que ejecuta el proceso ETL completo para la dimensión novedad.
    Extrae datos del OLTP, los transforma con registro especial y los carga en el Data Warehouse.
    """
    print("\nIniciando ETL para Dim_Novedad...")
    
    engine_oltp = get_oltp_engine()
    engine_dw = get_dw_engine()

    # Extracción desde OLTP
    df_oltp = extract_novedades_oltp(engine_oltp)
    
    # Transformación con registro especial
    df_dim = transform_novedades(df_oltp)
    
    # Carga hacia DW
    load_df_to_dw(df_dim, "Dim_Novedad", engine_dw, "Novedad_Key")
    
    print("Proceso de Dim_Novedad completado.")

if __name__ == "__main__":
    main()