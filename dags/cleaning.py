from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

"""
realizar trabajos en partes
"""

def task_1():
    print("-" * 25)
    print("task: Hola mundo : ) ")
    return "data_task1"
def task_2():
    print("-" * 25)
    print("task: Procesamiento ")
    return "data_task2"
def task_3():
    print("-" * 25)
    print("task: Realizado!")

with DAG(
    dag_id = 'dag_super_importante', # nombre que se verá en la UI
    start_date=datetime(2024, 1, 1), # desde cuándo se puede correr
    schedule_interval = None,  # trigger manual
    catchup = False, # No correr fechas pasadas
    tags=['tutorial']
) as dag:

    # Tareas definidas
    t1 = PythonOperator(
        task_id='saludo_inicial',
        python_callable=task_1
    )
    t2 = PythonOperator(
        task_id='procesamiento',
        python_callable=task_2
    )
    t3 = PythonOperator(
        task_id='fianlizado',
        python_callable=task_3
    )  

    t1 >> t2 >> t3