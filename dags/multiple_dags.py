# dags/multiple_dags.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def crear_tarea(nombre):
    def funcion():
        print(f"Ejecutando {nombre}")
    return funcion

# Crear 5 DAGs en un loop
for i in range(1, 6):
    dag_id = f'hello_world_{i}'
    
    with DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False
    ) as dag:
        
        tarea = PythonOperator(
            task_id='say_hello',
            python_callable=crear_tarea(dag_id)
        )
    
    # IMPORTANTE: Asignar el DAG a una variable global
    globals()[dag_id] = dag