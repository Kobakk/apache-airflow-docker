from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def task_1():
    print("-" * 25)
    print("task: extraer ventas")
    return "data_task1"
def task_2():
    print("-" * 25)
    print("task: extraer vendedor ")
    return "data_task2"
def task_3():
    print("-" * 25)
    print("task: Unir datos en ventas por vendedor")



with DAG(
    dag_id = 'primer_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 16, 10, 0), # Hoy
    schedule_interval= None,  # 6AM, 12PM, 6PM, 10PM
    catchup = False, # No correr fechas pasadas
    tags=['creacion_dag_basico']
) as dag:

    # Tareas definidas
    t1 = PythonOperator(
        task_id='datos_ventas',
        python_callable=task_1
    )
    t2 = PythonOperator(
        task_id='datos_vendedor',
        python_callable=task_2
    )
    t3 = PythonOperator(
        task_id='union_tablas',
        python_callable=task_3
    )  

    t1 >> t2 >> t3