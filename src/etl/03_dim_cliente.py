import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_clientes_oltp(engine_oltp):
    """
    Extrae los datos de los clientes desde la base de datos operacional.
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
    Aplica las transformaciones necesarias a los datos de los clientes.
    """
    # Para este caso, la transformación principal es generar la clave subrogada.
    # El resto de columnas ya vienen nombradas desde el SQL.
    df.insert(0, 'Cliente_Key', range(1, 1 + len(df)))
    
    # Asegurarnos de que no haya nulos no deseados si es necesario.
    # df['Industria_Cliente'] = df['Industria_Cliente'].fillna('No especificada')
    
    print("Transformación de clientes completada (generación de Key).")
    return df

def main():
    """
    Orquesta el proceso de ETL para la dimensión de clientes.
    """
    print("Iniciando ETL para Dim_Cliente...")
    
    # 1. Conectar a las bases de datos
    try:
        engine_oltp = get_oltp_engine()
        engine_dw = get_dw_engine()
        print("Conexiones a OLTP y DW establecidas.")
    except Exception as e:
        print(f"Error al conectar a las bases de datos: {e}")
        return

    # 2. Extraer datos del OLTP
    df_clientes_oltp = extract_clientes_oltp(engine_oltp)

    # 3. Transformar datos
    df_dim_cliente = transform_clientes(df_clientes_oltp)
    
    # 4. Cargar datos en el DW
    load_df_to_dw(df_dim_cliente, "Dim_Cliente", engine_dw, "Cliente_Key")
    
    print("Proceso de Dim_Cliente completado.")

if __name__ == "__main__":
    main() 