from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from read_data import preporcess
from merging_data import merged
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'extenso_assignment',
    default_args=default_args,
    description='A DAG to dump a MySQL table',
    schedule_interval='@once',
    catchup=False,
)

preparing_preprocessing = PythonOperator(
    task_id = "Python",
    python_callable = preporcess,
    dag = dag
)
final_df = PythonOperator(
    task_id = "final_df",
    python_callable = merged,
    dag = dag
)
preparing_preprocessing >> final_df




