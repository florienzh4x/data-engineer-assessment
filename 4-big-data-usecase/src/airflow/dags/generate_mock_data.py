from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from generator.data_generator import main

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': None,
}

with DAG(
    dag_id='generate_mock_data',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    description='DAG to run mock data generator',
    tags=['generator', 'mock', 'trigger'],
) as dag:
    
    generate_mock_data = PythonOperator(
        task_id='generate_mock_data',
        python_callable=main,
        dag=dag,
    )
    
    trigger_retail360_etl = TriggerDagRunOperator(
        task_id='trigger_retail360_etl',
        trigger_dag_id='retail360_etl',
        wait_for_completion=False,  # Set to True if you want to wait until the triggered DAG finishes
    )

    generate_mock_data >> trigger_retail360_etl