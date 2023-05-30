from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'yoda.jedi',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 22),
}

dag = DAG('alarming_system', default_args=default_args, schedule_interval='1 * * * *')

a = "curl -X POST -H 'Authorization: Bearer o5bKjcwyPcbj0rK30IroxMTPa7vp8YRXWJCpSIfSQCV' \
	-F '\nmessage= \n DAG이름 : {{dag.dag_id}}  Operator이름 : {{task.task_id}} 시작!' \
	https://notify-api.line.me/api/notify"

first = BashOperator(
	task_id = 'first',
	bash_command ="echo 1 &&" + a,
	dag = dag
)

last = BashOperator(
	task_id = 'last',
	bash_command ="echo 2",
	dag = dag
)

first >> last
