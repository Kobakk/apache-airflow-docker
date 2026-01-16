from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def tarea_post_sensor(**kwargs):
    print("fichero detectado!")
    print("ruta: /tmp/archivo.txt")
    print("pipeline procesada")

def crear_archivo_manual():
    with open('/tmp/archivo.txt', 'w') as f:
        f.write("Datos listos para procesar")
    print("Fichero creado para pruebas")

with DAG(
    dag_id='sensor_prueba',
    default_args=default_args,
    start_date=datetime(2026, 1, 16),
    schedule_interval=None,  
    catchup=False,
    tags=['sensor', 'tutorial']
) as dag:

    # SENSOR: Espera fichero externo (poke cada 10s, timeout 5min)
    sensor = FileSensor(
        task_id='esperar_fichero',
        filepath='/tmp/archivo_esperado.txt',
        
        # POKE MODE (ocupa worker)
        poke_interval=10,      # Chequea cada 10 segundos
        timeout=300,           # Falla tras 5 minutos
        mode='poke',           # Ocupa slot del worker
        
        soft_fail=False
    )
    
    # Tarea normal DESPUÉS del sensor
    procesar = PythonOperator(
        task_id='procesar_datos',
        python_callable=tarea_post_sensor
    )
    
    crear_prueba = PythonOperator(
        task_id='crear_archivo_prueba',
        python_callable=crear_archivo_manual
    )
    
    crear_prueba >> sensor >> procesar
