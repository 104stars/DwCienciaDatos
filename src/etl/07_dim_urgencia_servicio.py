import pandas as pd
from ..utils.db_connections import get_oltp_engine, get_dw_engine, load_df_to_dw

def extract_tipos_servicio_oltp(engine_oltp):
    """
    Extrae los datos de los tipos de servicio desde la base de datos operacional.
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
    Aplica las transformaciones necesarias a los datos de urgencia.
    """
    # Función para asignar la categoría de urgencia
    def asignar_categoria(descripcion):
        descripcion_lower = descripcion.lower()
        
        # Alta prioridad
        if 'urgente' in descripcion_lower or 'urgencia' in descripcion_lower or 'vital' in descripcion_lower:
            return 'Alta'
        
        # Media prioridad
        elif ('normal' in descripcion_lower or '2-3' in descripcion or 
              'comercial' in descripcion_lower or 'clínico' in descripcion_lower or 'clinico' in descripcion_lower):
            return 'Media'
        
        # Baja prioridad  
        elif 'administrativo' in descripcion_lower:
            return 'Baja'
        
        # Por defecto
        else:
            return 'Baja'

    df['Categoria_Urgencia'] = df['Descripcion_Urgencia'].apply(asignar_categoria)
    
    # Seleccionar y reordenar las columnas finales
    df = df[['Urgencia_ID_Operacional', 'Descripcion_Urgencia', 'Categoria_Urgencia']]
    
    # Generar la clave subrogada
    df.insert(0, 'Urgencia_Servicio_Key', range(1, 1 + len(df)))
    
    print("Transformación de urgencia de servicio completada.")
    return df

def main():
    """
    Orquesta el proceso de ETL para la dimensión de urgencia de servicio.
    """
    print("\nIniciando ETL para Dim_Urgencia_Servicio...")
    
    engine_oltp = get_oltp_engine()
    engine_dw = get_dw_engine()

    df_oltp = extract_tipos_servicio_oltp(engine_oltp)
    df_dim = transform_urgencia(df_oltp)
    
    load_df_to_dw(df_dim, "Dim_Urgencia_Servicio", engine_dw, "Urgencia_Servicio_Key")
    
    print("Proceso de Dim_Urgencia_Servicio completado.")

if __name__ == "__main__":
    main() 