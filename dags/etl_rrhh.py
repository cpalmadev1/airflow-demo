from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Argumentos por defecto del DAG
default_args = {
    'owner': 'cesar',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Funciones que simulan cada etapa del ETL
def extraer():
    print("Extrayendo archivo CSV de RRHH...") 
    datos = [
        {'id': 1, 'nombre': 'Juan', 'sueldo': 500000},
        {'id': 2, 'nombre': 'Maria', 'sueldo': None},   # dato inválido
        {'id': 3, 'nombre': 'Pedro', 'sueldo': 700000},
    ]
    print(f"Registros extraídos: {len(datos)}")
    return datos

def validar():
    print("Validando datos...")
    print("Filtrando registros con sueldo nulo...")
    print("Registros válidos: 2 de 3")

def cargar():
    print("Cargando datos limpios a PostgreSQL...")
    print("2 registros insertados correctamente")

# Definición del DAG
with DAG(
    dag_id='etl_rrhh',
    default_args=default_args,
    description='Pipeline ETL de recursos humanos',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['rrhh', 'etl']
) as dag:

    tarea_extraer = PythonOperator(
        task_id='extraer',
        python_callable=extraer
    )

    tarea_validar = PythonOperator(
        task_id='validar',
        python_callable=validar
    )

    tarea_cargar = PythonOperator(
        task_id='cargar',
        python_callable=cargar
    )

    # Define el orden de ejecución
    tarea_extraer >> tarea_validar >> tarea_cargar