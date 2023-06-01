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
        if aws s3 ls s3://{{ var.value.STORAGE_PATH }}stock/006800.csv; then
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
	bash_command = "aws s3 cp s3://{{ var.value.STORAGE_PATH }}stock/006800.csv /opt/airflow/stock/",
	dag = dag
)

move_tmpData = BashOperator(
	task_id = 'move_tmp',
	bash_command = """
		scp /opt/airflow/stock/006800.csv {{var.value.HADOOP_SERVER_IP}}:/home/yoda/data/tmp/006800.csv
	""",
	dag = dag
)

load_hdfs = BashOperator(
	task_id = 'load_hdfs',
	bash_command="""
		ssh {{var.value.HADOOP_SERVER_IP}} {{var.value.HDFS_CMD}}hdfs dfs \
		-put /home/yoda/data/tmp/* /user/yoda/hive/stock_pipe/ 
	""",
	dag = dag
)

create_hive_table = BashOperator(
	task_id = 'create_table',
	bash_command = """
		ssh {{var.value.HADOOP_SERVER_IP}} {{var.value.HIVE_CMD}} -f /home/yoda/hive/query/pipe.hql
	""",
	dag = dag
)

send_noti = BashOperator(
	task_id = 'fail_noti',
	trigger_rule = 'one_failed',
	bash_command = """
		curl -X POST -H 'Authorization: Bearer {{var.value.BEARER_TOKEN}}' \
        -F 'message= \n DAG이름 : {{dag.dag_id}} 실패! 일해라 노예야' \
        https://notify-api.line.me/api/notify
	""",
	dag = dag
)

first >> check_data >> load_toLocal >> move_tmpData >> load_hdfs >> create_hive_table
create_hive_table >> [send_noti, finish]


