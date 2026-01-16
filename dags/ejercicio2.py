from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# Argumentos por defecto
default_args = {
    'owner': 'formacion_airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'formacion_ejemplo_ejercicio_2',
    default_args=default_args,
    description='DAG para analizar Graph View, paralelismo y cuellos de botella',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ejercicio_2'],
) as dag:

    inicio = DummyOperator(task_id='inicio_proceso')

    extract_ventas = BashOperator(
        task_id='extraer_ventas',
        bash_command='sleep 5'
    )

    extract_inventario = BashOperator(
        task_id='extraer_inventario',
        bash_command='sleep 8'
    )

    extract_rrhh = BashOperator(
        task_id='extraer_rrhh',
        bash_command='sleep 4'
    )

    consolidacion = BashOperator(
        task_id='consolidar_datos_bottleneck',
        bash_command='echo "Uniendo todos los datos..."; sleep 3'
    )

    fin = DummyOperator(task_id='fin_proceso')

    # Definición de dependencias
    inicio >> [extract_ventas, extract_inventario, extract_rrhh] >> consolidacion >> fin