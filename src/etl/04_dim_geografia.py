import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_geografia_oltp(engine_oltp):
    """
    Extrae y une los datos de ciudades y departamentos desde la base de datos operacional.
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
    Aplica las transformaciones necesarias a los datos geográficos.
    """
    # Asignar valores por defecto según el schema.dbml
    df['Pais'] = 'Colombia'
    df['Zona_Geografica'] = None  # No hay fuente directa en el OLTP

    # Generar la clave subrogada
    df.insert(0, 'Geografia_Key', range(1, 1 + len(df)))
    
    print("Transformación de geografía completada.")
    return df

def main():
    """
    Orquesta el proceso de ETL para la dimensión de geografía.
    """
    print("\nIniciando ETL para Dim_Geografia...")
    
    # 1. Conectar a las bases de datos
    try:
        engine_oltp = get_oltp_engine()
        engine_dw = get_dw_engine()
        print("Conexiones a OLTP y DW establecidas.")
    except Exception as e:
        print(f"Error al conectar a las bases de datos: {e}")
        return

    # 2. Extraer datos del OLTP
    df_geografia_oltp = extract_geografia_oltp(engine_oltp)

    # 3. Transformar datos
    df_dim_geografia = transform_geografia(df_geografia_oltp)
    
    # 4. Cargar datos en el DW
    load_df_to_dw(df_dim_geografia, "Dim_Geografia", engine_dw, "Geografia_Key")
    
    print("Proceso de Dim_Geografia completado.")

if __name__ == "__main__":
    main() 