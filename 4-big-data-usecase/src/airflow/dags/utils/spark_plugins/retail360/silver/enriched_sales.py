import json
import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

def create_spark_session(config):

    app_name = "enchanted_sales"
    spark = SparkSession.builder.appName(app_name) \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .config("spark.hadoop.fs.s3a.endpoint", config['minio_host']) \
        .config("spark.hadoop.fs.s3a.access.key", config['minio_credentials']['access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['minio_credentials']['secret_key']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    return spark

def execute_transformation(spark_session, config):
    
    parquet_destination_path = f"{config['destination_bucket']}/{config['destination_prefix']}/"
    
    with open(config['sql_script_path'], 'r') as sql_file:
        sql_script = sql_file.read()
        
    for table in config['source_table']:
        
        # if table['source_type'] == 'json':
        #     df = spark_session.read.json(f"s3a://{table['source_bucket']}/{table['source_prefix']}")
        # elif table['source_type'] == 'csv':
        #     df = spark_session.read.csv(f"s3a://{table['source_bucket']}/{table['source_prefix']}", header=True, sep=table['separator'])
            
        if table['source_type'] == 'json':
            df = spark_session.read.json(f"/opt/airflow/dags/generator/generator_output/transactions.json")
        elif table['source_type'] == 'csv':
            df = spark_session.read.csv(f"/opt/airflow/dags/generator/generator_output/customers.csv", header=True, sep=table['separator'])
        
        df.createOrReplaceTempView(table['source_name'])
        
    result = spark_session.sql(sql_script)
    
    print(result.count())

    result.show()
    
    result.write.mode("overwrite").parquet(f"s3a://{parquet_destination_path}", compression='snappy')
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--configs", default="")

    args = parser.parse_args()

    configs = json.loads(args.configs)

    spark = create_spark_session(configs)

    execute_transformation(spark, configs)