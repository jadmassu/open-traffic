# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import datetime
# from operatores.load_data_operator import LoadDataOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# default_args = {
#     'start_date': datetime(2023, 12, 12),
#     'schedule_interval': '@daily',
# }

# def load_data_to_database(file_path):
#     # The logic here can be left empty since the actual data loading is handled by the LoadDataOperator
#     pass


# create_table_query = """
#     CREATE TABLE IF NOT EXISTS annotations (
#         Time numeric,
#         ID integer,
#         Type text,
#         x_img numeric,
#         y_img numeric,
#         Angle_img text
#     );
# """
# create_table_task = PostgresHook(postgres_conn_id='postgres_default').run(create_table_query)


# dag = DAG('data_loading_dag', default_args=default_args)

# # Specify the directory containing the data files
# data_directory = os.environ.get('AIRFLOW_PROJ_DIR', '..') + '/data/Annotations'

# # Get a list of all files in the directory
# files = os.listdir(data_directory)

# start_task = DummyOperator(task_id='start', dag=dag)
# end_task = DummyOperator(task_id='end', dag=dag)

# # Create a task for each file
# for file_name in files:
#     # Construct the full path of each file
#     file_path = os.path.join(data_directory, file_name)

#     # Create a LoadDataOperator for each file
#     task_id = f"load_data_{file_name}"
#     load_data_task = LoadDataOperator(
#         task_id=task_id,
#         postgres_conn_id='postgres_default',
#         source_path=file_path,
#         target_table='Annotations',
#         dag=dag
#     )

#     # Set the task dependencies
#     start_task >> load_data_task >> end_task



from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from operators.load_data import load_data
import os


default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

data_directory = os.environ.get('AIRFLOW_PROJ_DIR', '..') + '/data/Annotations'

table_name = 'airflow'

# DAG definition
dag = DAG('load_data_dag', default_args=default_args, schedule_interval='@daily')

# Task definition
load_data_operator = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    op_kwargs={'folder_path': data_directory, 'table_name': table_name},
    dag=dag
)

# Add the task to the DAG
load_data_operator