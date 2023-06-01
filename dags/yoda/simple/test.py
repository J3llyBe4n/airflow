from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

default_args = {
   'owner' : 'yoda_jedi',
   'depends_on_past': False,
   'start_date' : datetime(2023,5,27),
}

dag = DAG('test_dag', default_args = default_args, schedule_interval='@once')

def tt():
   name = '005930.KS'
   ticker = yf.Ticker(name)
   df = ticker.history(interval='1d', period='5d', auto_adjust=False)
   print(df)

first = EmptyOperator(
   task_id='start',
   dag = dag
)

end = EmptyOperator(
    task_id = 'finish',
    dag = dag
)

tt = PythonOperator(
   task_id = 'test',
   python_callable = tt,
   dag = dag
)

first >> tt >> end 
