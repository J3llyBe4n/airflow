from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from datetime import datetime

default_args = {
	'owner' : 'yoda_jedi',
	'depends_on_past' : False,
	'start_date': datetime(2023,6,2)
}

dag = DAG('convert_parquet', default_args = default_args, schedule_interval = '@once')

start = EmptyOperator(
	task_id = 'start_task',
	dag = dag
)

finish = EmptyOperator(
	task_id = 'finish_task',
	dag = dag
)

parquet_tmpTable = BashOperator(
	task_id = 'convert_table_toParquet',
	bash_command = """
		ssh yoda@192.168.90.128 \
		"/home/yoda/hive/apache-hive-3.1.2-bin/bin/hive -e \
		'CREATE TABLE period_table STORED AS parquet \
		AS SELECT * FROM pipe_stock WHERE stock_date LIKE \\"2016-%\\";'"
	""",
	dag = dag
)

load_local = BashOperator(
	task_id = 'load_parquet_table',
	bash_command = """
		ssh yoda@192.168.90.128 \
		"/home/yoda/hive/apache-hive-3.1.2-bin/bin/hive -e \
		'insert overwrite directory \'/home/yoda/data/parquets\' \
		select * from period_table;'""
	""",
	dag = dag
)

start >> parquet_tmpTable >> load_local >> finish