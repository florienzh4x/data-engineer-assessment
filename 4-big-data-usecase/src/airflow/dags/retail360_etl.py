from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import json

from utils.ingestion import IngestionUtils

dags_folder = conf.get('core', 'dags_folder')

CONFIG_DIR = os.path.join(dags_folder, 'config')

default_args = {
    "owner":"airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": None,
}

def job_creator(config, dag):
    tier = config['tier']
    
    if tier == 'bronze':
        
        ingestion_utils = IngestionUtils(config)
        
        return PythonOperator(
            task_id=f"{config['tier']}-{config['source_name']}-{config['job_name']}",
            python_callable=ingestion_utils.ingest_data,
            dag=dag
        )
    if tier == 'silver':
        
        return SparkSubmitOperator(
            task_id = f"{config['tier']}-{config['destination_table_name']}-{config['job_name']}",
            application=config['spark_job_script_path'],
            env_vars = {
                "HADOOP_CONF_DIR":"/etc/hadoop/conf"
            },
            execution_timeout=timedelta(minutes=30),
            application_args = [
                f'--configs', json.dumps(config),
                ],
            jars=f"{config['jars']}",
            dag=dag
        )

def config_reader(module_name, tier, table_name):
    
    with open(f'{CONFIG_DIR}/{module_name}/{tier}/{table_name}', 'rt') as outfile:

        config_data = outfile.read()
        config_data = json.loads(config_data)

    return config_data


def module_config_reader(module_name, tier):

    with TaskGroup(group_id=f"{module_name}_{tier}") as tg:

        list_config = os.listdir(f'{CONFIG_DIR}/{module_name}/{tier}')
        print(list_config)

        for iter_list_config in list_config:

            config = config_reader(module_name, tier, iter_list_config)

            job_creator(config, dag)

        return tg


with DAG(
    dag_id='retail360_etl',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    description='DAG to run retail360 ETL',
    tags=['retail360', 'ETL'],
) as dag:
    
    bronze_layer = module_config_reader('retail360', 'bronze')

    silver_layer = module_config_reader('retail360', 'silver')

    # gold_layer = module_config_reader('retail360', 'gold')
    
    bronze_layer >> silver_layer