import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_tipos_servicio_oltp(engine_oltp):
    """
    Extrae información de tipos de servicio desde la base de datos OLTP.
    
    Args:
        engine_oltp (sqlalchemy.Engine): Motor de conexión al sistema OLTP
    
    Returns:
        pd.DataFrame: DataFrame con los datos de tipos de servicio extraídos
    """
    query = """
    SELECT
        id AS "Urgencia_ID_Operacional",
        nombre AS "Descripcion_Urgencia"
    FROM
        public.mensajeria_tiposervicio
    """
    df = pd.read_sql(query, engine_oltp)
    print(f"Se extrajeron {len(df)} registros de tipos de servicio desde el OLTP.")
    return df

def transform_urgencia(df):
    """
    Transforma los datos de urgencia de servicio aplicando categorización automática
    basada en palabras clave en la descripción y agregando una clave primaria surrogate.
    
    Args:
        df (pd.DataFrame): DataFrame con datos de tipos de servicio del OLTP
    
    Returns:
        pd.DataFrame: DataFrame transformado con categorías de urgencia y clave primaria surrogate
    """
    def asignar_categoria(descripcion):
        """
        Asigna una categoría de urgencia basada en palabras clave en la descripción.
        
        Args:
            descripcion (str): Descripción del tipo de servicio
        
        Returns:
            str: Categoría de urgencia ('Alta', 'Media', 'Baja')
        """
        descripcion_lower = descripcion.lower()
        
        if 'urgente' in descripcion_lower or 'urgencia' in descripcion_lower or 'vital' in descripcion_lower:
            return 'Alta'
        
        elif ('normal' in descripcion_lower or '2-3' in descripcion or 
              'comercial' in descripcion_lower or 'clínico' in descripcion_lower or 'clinico' in descripcion_lower):
            return 'Media'
        
        elif 'administrativo' in descripcion_lower:
            return 'Baja'
        
        else:
            return 'Baja'

    # Aplicar categorización automática
    df['Categoria_Urgencia'] = df['Descripcion_Urgencia'].apply(asignar_categoria)
    
    # Seleccionar columnas finales
    df = df[['Urgencia_ID_Operacional', 'Descripcion_Urgencia', 'Categoria_Urgencia']]
    
    # Agregar clave primaria surrogate
    df.insert(0, 'Urgencia_Servicio_Key', range(1, 1 + len(df)))
    
    print("Transformación de urgencia de servicio completada.")
    return df

def main():
    """
    Función principal que ejecuta el proceso ETL completo para la dimensión urgencia de servicio.
    Extrae datos del OLTP, los transforma con categorización y los carga en el Data Warehouse.
    """
    print("\nIniciando ETL para Dim_Urgencia_Servicio...")
    
    engine_oltp = get_oltp_engine()
    engine_dw = get_dw_engine()

    # Extracción desde OLTP
    df_oltp = extract_tipos_servicio_oltp(engine_oltp)
    
    # Transformación con categorización
    df_dim = transform_urgencia(df_oltp)
    
    # Carga hacia DW
    load_df_to_dw(df_dim, "Dim_Urgencia_Servicio", engine_dw, "Urgencia_Servicio_Key")
    
    print("Proceso de Dim_Urgencia_Servicio completado.")

if __name__ == "__main__":
    main() 