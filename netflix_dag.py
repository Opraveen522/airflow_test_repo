from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from netflix_tranformed import transform_csv_to_uppercase

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'retries': 1,
}

dag = DAG(
    'uppercase',
    default_args=default_args,
    description='A DAG to transform CSV file to uppercase',
    schedule_interval=None,
)

input_path =  "netflix_titles.csv"
output_path = "netflix_output.csv"

transform_task = PythonOperator(
    task_id='transform_csv_to_uppercase',
    python_callable=transform_csv_to_uppercase,
    op_kwargs={'input_file_path': input_path, 'output_file_path': output_path},
    dag=dag,
)

transform_task

