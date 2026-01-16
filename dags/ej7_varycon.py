from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {'retries': 1, 'retry_delay': timedelta(minutes=2)}

def usar_variable():
    bucket = Variable.get("BUCKET_NAME")
    print(f"bucket configurado: {bucket}")
    print("confi externa cargada")

with DAG(
    dag_id='variables_connections',
    default_args=default_args,
    start_date=datetime(2026, 1, 16),
    schedule_interval=None,
    catchup=False,
    tags=['variables', 'connections']
) as dag:

    t1 = PythonOperator(
        task_id='leer_variable',
        python_callable=usar_variable
    )
    
    t2 = GCSListObjectsOperator(
        task_id='listar_gcs',
        bucket="{{ var.value.BUCKET_NAME }}",  # ← Jinja + Variable
        google_cloud_conn_id='gcp_conn',       # ← Connection ID
    )
    
    t1 >> t2
