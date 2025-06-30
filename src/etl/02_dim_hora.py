import pandas as pd
from sqlalchemy import create_engine, text
from datetime import time

def get_franja_horaria(hora):
    """Devuelve la franja horaria según la hora del día."""
    if 0 <= hora <= 5:
        return 'Madrugada'
    elif 6 <= hora <= 11:
        return 'Mañana'
    elif 12 <= hora <= 17:
        return 'Tarde'
    else: # 18 a 23
        return 'Noche'

def generar_dimension_hora():
    """
    Genera un DataFrame con la dimensión de hora para cada minuto del día.
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
    Carga un DataFrame en una tabla del Data Warehouse SQLite, estableciendo la PK.
    """
    # En SQLite, la mejor forma de establecer una PK con to_sql es usar el índice del DataFrame.
    df_con_pk = df.set_index(pk_column, drop=True)
    
    with engine_dw.connect() as connection:
        # Usamos if_exists='replace' para que el ETL sea idempotente durante el desarrollo.
        # index=True para escribir el índice del DataFrame como una columna.
        # index_label=pk_column le da el nombre correcto a esa columna.
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
    Orquesta la generación y carga de la dimensión de hora.
    """
    # --- Parámetros de Conexión al DW ---
    # Para SQLite, la conexión es la ruta a un archivo.
    db_path_dw = "DW_FastAndSafe.db"
    connection_string_dw = f"sqlite:///{db_path_dw}"
    engine_dw = create_engine(connection_string_dw)

    # --- Generación de Horas ---
    print("Generando dimensión de hora...")
    df_dim_hora = generar_dimension_hora()
    
    # Agregar la clave subrogada (Key)
    df_dim_hora.insert(0, 'Hora_Key', range(1, 1 + len(df_dim_hora)))
    
    # --- Carga en el DW ---
    print("Cargando dimensión de hora en el Data Warehouse...")
    cargar_datos_dw(df_dim_hora, "Dim_Hora", engine_dw, "Hora_Key")
    print("Proceso de Dim_Hora completado.")

if __name__ == "__main__":
    main() 