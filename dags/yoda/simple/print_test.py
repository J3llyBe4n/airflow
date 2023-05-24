from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_test():
	now_date = datetime.now()
	print("test: %s" %now_date)

default_args = {
    'owner': 'yoda.jedi',
    'start_date': datetime(2023, 5, 22),
}


dag = DAG('test123_dag', default_args= default_args, schedule_interval = '1 * * * *')

task = PythonOperator(
    task_id='print_test_task',
    python_callable=print_test,
    dag=dag
)

task
