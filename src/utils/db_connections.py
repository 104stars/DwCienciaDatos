from sqlalchemy import create_engine

def get_dw_engine():
    """
    Crea y devuelve un motor de SQLAlchemy para el Data Warehouse (SQLite).
    """
    db_path_dw = "DW_FastAndSafe.db"
    connection_string = f"sqlite:///{db_path_dw}"
    engine = create_engine(connection_string)
    return engine

def get_oltp_engine():
    """
    Crea y devuelve un motor de SQLAlchemy para la base de datos OLTP (PostgreSQL).
    ¡IMPORTANTE! Debes reemplazar con tus credenciales reales.
    """
    db_user_oltp = "postgres"
    db_password_oltp = "1"
    db_host_oltp = "localhost"
    db_port_oltp = "5432"
    db_name_oltp = "DW_FastAndSafe"

    connection_string = f"postgresql://{db_user_oltp}:{db_password_oltp}@{db_host_oltp}:{db_port_oltp}/{db_name_oltp}"
    engine = create_engine(connection_string)
    return engine

def load_df_to_dw(df, table_name, engine, pk_column):
    """
    Carga un DataFrame en una tabla del Data Warehouse (SQLite), estableciendo la PK.
    """
    df_with_pk = df.set_index(pk_column, drop=True)
    
    with engine.connect() as connection:
        df_with_pk.to_sql(
            table_name, 
            connection, 
            if_exists='replace', 
            index=True, 
            index_label=pk_column,
            chunksize=1000
        )
    print(f"Tabla '{table_name}' cargada exitosamente en el DW.")
    print(f"Clave primaria '{pk_column}' establecida en '{table_name}'.") 