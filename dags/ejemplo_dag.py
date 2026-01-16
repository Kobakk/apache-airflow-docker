from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def funcion_que_falla():
    print("Iniciando proceso crítico...")
    raise ValueError("Error simulado: No se pudo conectar a la base de datos externa.")

default_args = {
    'owner': 'formacion_airflow',
    'retries': 0, 
}

with DAG(
    'formacion_ejemplo_dags_fallos',
    default_args=default_args,
    description='DAG diseñado para fallar y analizar logs',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    tarea_ok = PythonOperator(
        task_id='verificacion_inicial',
        python_callable=lambda: print("Todo parece correcto al inicio.")
    )

    tarea_error = PythonOperator(
        task_id='procesamiento_critico',
        python_callable=funcion_que_falla
    )

    tarea_final = PythonOperator(
        task_id='reporte_final',
        python_callable=lambda: print("Este paso no debería ejecutarse.")
    )

    tarea_ok >> tarea_error >> tarea_final