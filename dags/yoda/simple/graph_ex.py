from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'yoda.jedi',
    'depends_on_past' : False,
    'start_date': datetime(2023, 5, 22),
}

dag = DAG('graph_ex_dag', default_args= default_args, schedule_interval = '1 * * * *')

task_2 = BashOperator(
    task_id = 'task_2',
    bash_command="""
        echo "Start";
        echo "completed"
    """,
    dag = dag
)

last = EmptyOperator(
	task_id='finish',
	dag = dag
)

first = EmptyOperator(
	task_id='start',
	dag = dag
)


task_1 = BashOperator(
    task_id = 'task_1',
    bash_command="""
        echo "Start";
        echo "completed"
    """,
    dag = dag
)


first >> [task_1, task_2] >> last
