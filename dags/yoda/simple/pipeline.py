from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'yoda.jedi',
    'depends_on_past' : False,
    'start_date': datetime(2023, 5, 22),
}

#dag = DAG('pipeline_dag', default_args= default_args, schedule_interval = '*/1 * * * *')

dag = DAG('pipeline_dag', default_args= default_args, schedule_interval = '0 0 * * *')

first = EmptyOperator(
	task_id='start',
	dag = dag
)

first_alarm = BashOperator(
	task_id = 'start_alarm',
	bash_command = "curl -X POST -H 'Authorization: Bearer o5bKjcwyPcbj0rK30IroxMTPa7vp8YRXWJCpSIfSQCV' -F '\nmessage= \n DAG이름 : {{dag.dag_id}}  Operator이름 : {{task.task_id}} 시작!' https://notify-api.line.me/api/notify",
	dag = dag
)

load_data = BashOperator(
	task_id = 'load_sample_data',
    bash_command="""
        cat /opt/airflow/dags/data/web.log | grep "item=" > /opt/airflow/dags/data/first.log
    """,
    dag = dag
)

filtering_first = BashOperator(
	task_id = 'first_pipeline',
	bash_command="""
		cat /opt/airflow/dags/data/first.log |grep -v "INFO" > /opt/airflow/dags/data/second.log
	""",
	dag = dag
)


filtering_second = BashOperator(
	task_id = 'second_pipeline',
	bash_command="""
		cat /opt/airflow/dags/data/second.log |cut -d"," -f 1 | cut -d"=" -f 2 > /opt/airflow/dags/data/RAW.log 
	""",
	dag = dag
)

summary_task = BashOperator(
	task_id = 'summary_task',
	bash_command="""
		cat /opt/airflow/dags/data/RAW.log | sort -n | uniq -c > /opt/airflow/dags/data/SUM.log 
	""",
	dag = dag
)

last = EmptyOperator(
	task_id ='last',
	dag = dag
)

first >> first_alarm >> load_data >> filtering_first >> filtering_second >> summary_task >>last