'''
3 tareas secuenciales
ejecución diaria 
catchup = False
 '''



with DAG(
    dag_id = 'primer_pipeline', # nombre que se verá en la UI
    start_date=datetime(2024, 1, 1), # desde cuándo se puede correr
    schedule_interval = None,  # trigger manual
    catchup = False, # No correr fechas pasadas
    tags=['tutorial']
) as dag:
