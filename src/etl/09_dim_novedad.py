import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_novedades_oltp(engine_oltp):
    """
    Extrae los datos de los tipos de novedad desde la base de datos operacional.
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
    Aplica las transformaciones necesarias a los datos de las novedades,
    y añade el registro para 'Sin Novedad'.
    """
    # Asignar una categoría por defecto
    df['Categoria_Novedad'] = 'General'
    
    # Seleccionar las columnas que nos interesan
    df = df[['Novedad_ID_Operacional', 'Descripcion_Novedad', 'Categoria_Novedad']]
    
    # Añadir el registro para "Sin Novedad"
    sin_novedad = pd.DataFrame([{
        'Novedad_ID_Operacional': -1, # Usamos -1 como ID para "Sin Novedad"
        'Descripcion_Novedad': 'Sin Novedad',
        'Categoria_Novedad': 'Ninguna'
    }])
    df = pd.concat([sin_novedad, df], ignore_index=True)

    # Generar la clave subrogada, asegurando que "Sin Novedad" tenga la Key = 1
    df.insert(0, 'Novedad_Key', range(1, 1 + len(df)))
    
    print("Transformación de novedades completada.")
    return df

def main():
    """
    Orquesta el proceso de ETL para la dimensión de novedades.
    """
    print("\nIniciando ETL para Dim_Novedad...")
    
    engine_oltp = get_oltp_engine()
    engine_dw = get_dw_engine()

    df_oltp = extract_novedades_oltp(engine_oltp)
    df_dim = transform_novedades(df_oltp)
    
    load_df_to_dw(df_dim, "Dim_Novedad", engine_dw, "Novedad_Key")
    
    print("Proceso de Dim_Novedad completado.")

if __name__ == "__main__":
    main()