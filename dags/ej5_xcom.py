from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def generar_ruta(**kwargs):
    ruta = "/opt/airflow/dags/productos_2026-01-16.csv"
    print(f"Ruta: {ruta}")
    return ruta  # Se guarda en XCom
def usar_ruta(**kwargs):
    ruta_recibida = kwargs['task_instance'].xcom_pull(task_ids='generar_ruta')
    
    print(f"ruta recibida por XCom: {ruta_recibida}")
    print(f"procesando el fichero: {ruta_recibida}")
    print(f"peso del archivo : 2MB")

with DAG(
    dag_id='xcom_ejemplo',
    default_args=default_args,
    start_date=datetime(2026, 1, 16),
    schedule_interval=None,
    catchup=False,
    tags=['xcom', 'tutorial']
) as dag:

    t1 = PythonOperator(
        task_id='generar_ruta',
        python_callable=generar_ruta,
    )
    
    # TAREA 2: Recupera XCom y loguea
    t2 = PythonOperator(
        task_id='usar_ruta',
        python_callable=usar_ruta,
    )
    
    t1 >> t2