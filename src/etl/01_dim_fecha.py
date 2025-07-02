import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, timedelta

def generar_dimension_fecha(fecha_inicio, fecha_fin):
    """
    Genera un DataFrame con todas las fechas entre dos fechas dadas,
    incluyendo atributos temporales calculados.
    
    Args:
        fecha_inicio (date): Fecha inicial del rango
        fecha_fin (date): Fecha final del rango (inclusive)
    
    Returns:
        pd.DataFrame: DataFrame con las fechas y sus atributos temporales
    """
    fechas = []
    
    delta = fecha_fin - fecha_inicio
    
    for i in range(delta.days + 1):
        dia = fecha_inicio + timedelta(days=i)
        fechas.append({
            'Fecha_Completa': dia,
            'Ano': dia.year,
            'Trimestre': (dia.month - 1) // 3 + 1,
            'Numero_Mes': dia.month,
            'Nombre_Mes': dia.strftime('%B'),
            'Numero_Dia_Mes': dia.day,
            'Numero_Dia_Semana': dia.weekday() + 1,
            'Nombre_Dia_Semana': dia.strftime('%A'),
            'Es_Fin_Semana': dia.weekday() >= 5
        })
        
    df = pd.DataFrame(fechas)
    
    meses_es = {
        'January': 'Enero', 'February': 'Febrero', 'March': 'Marzo', 'April': 'Abril',
        'May': 'Mayo', 'June': 'Junio', 'July': 'Julio', 'August': 'Agosto',
        'September': 'Septiembre', 'October': 'Octubre', 'November': 'Noviembre', 'December': 'Diciembre'
    }
    dias_es = {
        'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles', 'Thursday': 'Jueves',
        'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
    }
    
    df['Nombre_Mes'] = df['Nombre_Mes'].map(meses_es)
    df['Nombre_Dia_Semana'] = df['Nombre_Dia_Semana'].map(dias_es)

    return df

def cargar_datos_dw(df, nombre_tabla, engine_dw, pk_column):
    """
    Carga un DataFrame en el Data Warehouse con una clave primaria específica.
    
    Args:
        df (pd.DataFrame): DataFrame a cargar
        nombre_tabla (str): Nombre de la tabla destino
        engine_dw (sqlalchemy.Engine): Motor de conexión al DW
        pk_column (str): Nombre de la columna clave primaria
    """
    df_con_pk = df.set_index(pk_column, drop=True)
    
    with engine_dw.connect() as connection:
        df_con_pk.to_sql(
            nombre_tabla, 
            connection, 
            if_exists='replace', 
            index=True, 
            index_label=pk_column,
            chunksize=1000
        )
        print(f"Tabla '{nombre_tabla}' cargada exitosamente en el Data Warehouse.")
        print(f"Clave primaria '{pk_column}' establecida en '{nombre_tabla}'.")


def main():
    """
    Función principal que ejecuta el proceso ETL completo para la dimensión fecha.
    Genera fechas desde 2023 hasta 2025 y las carga en el Data Warehouse.
    """
    db_path_dw = "DW_FastAndSafe.db"
    connection_string_dw = f"sqlite:///{db_path_dw}"
    engine_dw = create_engine(connection_string_dw)

    fecha_inicio = date(2023, 1, 1)
    fecha_fin = date(2025, 12, 31)
    
    print("Generando dimensión de fecha...")
    df_dim_fecha = generar_dimension_fecha(fecha_inicio, fecha_fin)
    
    df_dim_fecha.insert(0, 'Fecha_Key', range(1, 1 + len(df_dim_fecha)))
    
    print("Cargando dimensión de fecha en el Data Warehouse...")
    cargar_datos_dw(df_dim_fecha, "Dim_Fecha", engine_dw, "Fecha_Key")
    print("Proceso de Dim_Fecha completado.")

if __name__ == "__main__":
    main() 