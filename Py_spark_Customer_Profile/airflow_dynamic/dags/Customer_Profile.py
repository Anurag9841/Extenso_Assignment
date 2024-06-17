from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from final_table import main
from aggregate_function import main1
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'email_on_failure': False,
    'email_on_retry': False,
}
spark_config = {
    'conn_id': 'spark_local',
    'application': '/opt/airflow/python_files/aggregate_function.py'
}
dag = DAG(
    'table_creation',
    default_args=default_args,
    description='A dag for customer_profile',
    schedule_interval='@once',
    catchup=False,
)
# table_creation = PythonOperator(
#     task_id = "table_creation",
#     python_callable = main,
#     dag = dag,
# )
# final_output = PythonOperator(
#     task_id = "final_output",
#     python_callable = main1,
#     dag = dag,
# )
spark_operator = SparkSubmitOperator(task_id='spark_operator', dag=dag, spark_conf=spark_config)
spark_operator
# table_creation >> final_output
# preparing_preprocessing >> final_df




