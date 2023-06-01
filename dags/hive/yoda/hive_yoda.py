from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from datetime import datetime

default_args = {
	'owner': 'yoda_jedi',
	'depends_on_past' : False,
	'start_date': datetime(2023,6,1),
}

dag = DAG('stock_pipeline', default_args = default_args, schedule_interval = '@once')

first = EmptyOperator(
	task_id = 'start_task',
	dag = dag
)

finish = EmptyOperator(
	task_id = 'finish_task',
	dag = dag
)

check_data = BashOperator(
	task_id = 'check_data_task',
	retries = 5,
	bash_command="""
        if aws s3 ls s3://{{ var.value.STORAGE_PATH }}stock/0068001.csv; then
            echo "file_exist"
            exit 0
        else
            echo "file_!exist"
            exit 1
        fi
    """,
	dag = dag
)

load_toLocal = BashOperator(
	task_id = 'loadData_to_worker',
	bash_command = "aws s3 cp s3://pd24/yoda/data/stock/006800.csv /opt/airflow/stock/",
	dag = dag
)



first >> check_data >> load_toLocal >>finish
