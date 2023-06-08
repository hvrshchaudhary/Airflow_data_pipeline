import json
import datetime
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract_data import extract_data
from transform_data import transform_data

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 2),
}

# Create the DAG object
dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='An end-to-end data pipeline using Airflow and Python',
    schedule=timedelta(minutes=30),
    catchup=False,
)

def save_to_s3(data, **kwargs):
    """
    This function saves the transformed data to an S3 bucket.
    
    Args:
        data (list): A list of dictionaries containing the transformed data.
           """
    import boto3
    execution_date = kwargs['execution_date']
    s3 = boto3.client('s3')
    bucket_name = 'api-data-from-airflow'
    execution_date = datetime.strptime(execution_date, "%Y-%m-%dT%H:%M:%S.%f%z")

    timestamp = execution_date.strftime("%Y%m%d-%H%M%S")
    key = f'transformed_data_{timestamp}.json'
    
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data).encode('utf-8')
    )

# Define the extract_data task
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Define the transform_data task
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Define the save_to_s3 task
save_task = PythonOperator(
    task_id='save_to_s3',
    python_callable=save_to_s3,
    op_args=['{{ ti.xcom_pull(task_ids="transform_data") }}'],
    op_kwargs={'execution_date': '{{ ts }}'},
    dag=dag,
)

# Set the task dependencies
extract_task >> transform_task >> save_task