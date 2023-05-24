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

# ssh 1호기 연결 bash 
# ssh 1호기 데이터 긁어서 worker_node datas에 적재 
# 데이터 처리 3단계 (전에 댓글 스크립트 확인 링크 : https://github.com/d-mario24/etl/pull/13#issuecomment-1556688165)
# 데이터 처리한놈들 2호기 서버 던지기