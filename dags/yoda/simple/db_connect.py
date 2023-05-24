from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import mysql.connector


default_args = {
    'owner': 'yoda.jedi',
    'depends_on_past' : False,
    'start_date': datetime(2023, 5, 22),
}

dag = DAG('mysql_dag', default_args= default_args, schedule_interval = '1 * * * *')

def connectSQLServer():
	conn = mysql.connector.connect(user='root', password= 'tmzkdnxj1', host='34.64.214.96', database = 'scout', port = '3306')
	print("Hi! SQL")
	cursor = conn.cursor()
	query = "select * from pipe_round where api_fixture_id = 867489"
	cursor.execute(query)
	results = cursor.fetchall()
	
	for row in results:
		print(row)

	conn.close()


t1 = PythonOperator(
	task_id = 'test',
	python_callable=connectSQLServer,
	dag = dag
)

t1
