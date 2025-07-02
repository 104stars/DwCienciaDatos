import pandas as pd
from sqlalchemy import create_engine, text
from datetime import time

def get_franja_horaria(hora):
    """
    Clasifica una hora del día en franjas horarias descriptivas.
    
    Args:
        hora (int): Hora del día (0-23)
    
    Returns:
        str: Nombre de la franja horaria correspondiente
    """
    if 0 <= hora <= 5:
        return 'Madrugada'
    elif 6 <= hora <= 11:
        return 'Mañana'
    elif 12 <= hora <= 17:
        return 'Tarde'
    else:
        return 'Noche'

def generar_dimension_hora():
    """
    Genera un DataFrame con todas las combinaciones de hora y minuto del día,
    incluyendo la clasificación por franja horaria.
    
    Returns:
        pd.DataFrame: DataFrame con 1440 registros (24 horas x 60 minutos)
    """
    horas = []
    for h in range(24):
        for m in range(60):
            hora_completa = time(h, m)
            horas.append({
                'Hora_Completa': hora_completa,
                'Hora_Del_Dia': h,
                'Minuto_De_La_Hora': m,
                'Franja_Horaria': get_franja_horaria(h)
            })
            
    df = pd.DataFrame(horas)
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
    Función principal que ejecuta el proceso ETL completo para la dimensión hora.
    Genera todas las horas y minutos del día y las carga en el Data Warehouse.
    """
    db_path_dw = "DW_FastAndSafe.db"
    connection_string_dw = f"sqlite:///{db_path_dw}"
    engine_dw = create_engine(connection_string_dw)

    print("Generando dimensión de hora...")
    df_dim_hora = generar_dimension_hora()
    
    # Agregar clave primaria surrogate
    df_dim_hora.insert(0, 'Hora_Key', range(1, 1 + len(df_dim_hora)))
    
    print("Cargando dimensión de hora en el Data Warehouse...")
    cargar_datos_dw(df_dim_hora, "Dim_Hora", engine_dw, "Hora_Key")
    print("Proceso de Dim_Hora completado.")

if __name__ == "__main__":
    main() 