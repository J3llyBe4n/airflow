from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'yoda.jedi',
    'depends_on_past' : False,
    'start_date': datetime(2023, 1, 1),
}

#dag = DAG('pipeline_dag', default_args= default_args, schedule_interval = '*/1 * * * *')

dag = DAG('enhance_pipeline_dag', default_args= default_args, schedule_interval = '0 5 * * *')

first = EmptyOperator(
	task_id='start',
	dag = dag
)

end = EmptyOperator(
    task_id = 'finish',
    dag = dag
)

get_data = BashOperator(
    task_id = 'get-data',
    bash_command = """
        scp -i /opt/airflow/dags/data/pd24-linux-101.pem ubuntu@52.79.253.139:/home/ubuntu/app/web/web.log /opt/airflow/dags/data/
    """,
    dag = dag
)

make_line_log = BashOperator(
    task_id = 'initialized_log_line',
    bash_command ="""
        if [ ! -f /opt/airflow/dags/data/line.log ]; then
            echo "================>"
            echo "$(date)-0" > /opt/airflow/dags/data/line.log
        else
            echo "line.log already exists. Skipping initialization."
        fi
    """,
    dag = dag
)

# 증분계산 필요 값 적재 
load_logline = BashOperator(
    task_id = 'read_data--load_lineCount',
    bash_command ="""
        echo "$(date)-$(cat /opt/airflow/dags/data/web.log | wc -l)" >> /opt/airflow/dags/data/line.log
    """,
    dag = dag
)

# 증분 계산 해주기
cal_add = BashOperator(
    task_id = 'load_additional_data',
    bash_command = """
        start_line=$(tail -n 2 /opt/airflow/dags/data/line.log | head -n 1 | cut -d '-' -f 2);
        end_line=$(tail -n 1 /opt/airflow/dags/data/line.log | cut -d '-' -f 2);
        head -n "$end_line" /opt/airflow/dags/data/web.log | tail -n +"$start_line" > /opt/airflow/dags/data/append.log
    """,
    dag = dag
)   

filter_first = BashOperator(
    task_id = 'filtering_item',
    bash_command ="""
        cat /opt/airflow/dags/data/append.log | grep "item=" > /opt/airflow/dags/data/tmp1.log
    """,
    dag = dag
)

filter_second = BashOperator(
    task_id = 'filtering_info',
    bash_command = """
        cat /opt/airflow/dags/data/tmp1.log |grep -v "INFO" > /opt/airflow/dags/data/tmp2.log
    """,
    dag = dag
)

filter_third = BashOperator(
    task_id = 'cut_data_first',
    bash_command ="""
        cat /opt/airflow/dags/data/tmp2.log | cut -d "=" -f 2 > /opt/airflow/dags/data/tmp3.log
    """,
    dag = dag
)

filter_fourth = BashOperator(
    task_id = 'cut_data_second',
    bash_command ="""
        cat /opt/airflow/dags/data/tmp3.log | cut -d "," -f 1 >> /opt/airflow/dags/data/RAW.log
    """,
    dag = dag
)

summary_data = BashOperator(
    task_id = 'cal_raw_data',
    bash_command = "cat /opt/airflow/dags/data/RAW.log | sort -n | uniq -c > /opt/airflow/dags/data/SUM.log",
    dag = dag 
)

delete_tmp = BashOperator(
    task_id = 'delete_tmpData',
    bash_command = """
        rm /opt/airflow/dags/data/tmp1.log;
        rm /opt/airflow/dags/data/tmp2.log;
        rm /opt/airflow/dags/data/tmp3.log;
        rm /opt/airflow/dags/data/web.log
        """,
    dag = dag
)

install_s3_cli = BashOperator(
    task_id = 's3_install',
    bash_command ="pip install awscli",
    dag = dag
) 

load_dataLake = BashOperator(
    task_id='load_toS3',
    bash_command="""
        aws s3 cp /opt/airflow/dags/data/SUM.log s3://pd24/yoda/{{ds_nodash}}/SUM.log;
        aws s3 cp /opt/airflow/dags/data/RAW.log \
        s3://pd24/yoda/{{execution_date.day}}/{{execution_date.month}}/{{execution_date.year}}/RAW.log
    """,
    dag=dag
)

done_flag = BashOperator(
    task_id = 'send_doneFlag',
    bash_command = """
        echo > /opt/airflow/dags/data/DONE;
        aws s3 cp /opt/airflow/dags/data/DONE s3://pd24/yoda/DONE/{{ds_nodash}}/_DONE
    """,
    dag = dag
)

first >> [get_data, install_s3_cli]
get_data >> make_line_log >>load_logline >> cal_add >> filter_first >> filter_second >> filter_third
filter_third >> filter_fourth >> summary_data >> delete_tmp 
[install_s3_cli, delete_tmp] >> load_dataLake >> done_flag >> end

# ssh 1호기 연결 bash -> v
# ssh 1호기 데이터 긁어서 worker_node datas에 적재 -> v
# 데이터 처리 3단계 (전에 댓글 스크립트 확인 링크 : https://github.com/d-mario24/etl/pull/13#issuecomment-1556688165)
# 데이터 처리한놈들 2호기 서버 던지기