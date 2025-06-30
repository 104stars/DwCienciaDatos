# src/run_etl.py
import importlib

def run_etl_script(script_name):
    """
    Importa y ejecuta la función 'main' de un script de ETL dado.
    """
    try:
        print(f"--- Ejecutando: {script_name} ---")
        module = importlib.import_module(f".etl.{script_name}", package="src")
        module.main()
        print(f"--- {script_name} completado. ---\n")
    except Exception as e:
        print(f"¡ERROR en {script_name}!: {e}")
        # Detener la ejecución si un script falla
        raise

def main():
    """
    Orquesta la ejecución de todos los scripts ETL en el orden correcto.
    """
    print("=========================================")
    print("=   INICIANDO PROCESO ETL COMPLETO      =")
    print("=========================================\n")
    
    # Lista de scripts a ejecutar en orden
    etl_scripts = [
        "01_dim_fecha",
        "02_dim_hora",
        "03_dim_cliente",
        "04_dim_geografia",
        "05_dim_sede",
        "06_dim_mensajero",
        "07_dim_urgencia_servicio",
        "08_dim_estado_servicio",
        "09_dim_novedad",
        "10_fact_cambio_estado_servicio"
    ]
    
    try:
        for script in etl_scripts:
            run_etl_script(script)
        
        print("\n=========================================")
        print("=    PROCESO ETL COMPLETADO CON ÉXITO   =")
        print("=========================================")
        
    except Exception:
        print("\n=========================================")
        print("=     PROCESO ETL DETENIDO POR ERROR    =")
        print("=========================================")

if __name__ == "__main__":
    main() 