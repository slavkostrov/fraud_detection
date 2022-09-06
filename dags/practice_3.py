"""
    Даг для обновления данных
"""
import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from scripts.tasks import *

from config import BaseETLConfig
config = BaseETLConfig()

with DAG(
        'practice_3',
        default_args={
            'depends_on_past': False,
            'email': ['slavkotrov@google.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
        },
        description='Simple ETL dag for practice_3.',
        schedule_interval='0 0 1 * *',
        start_date=datetime.datetime(2022, 7, 30, 0),
        catchup=False,
        tags=['otus_practice'],
) as dag:
    dag.doc_md = __doc__

    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    check_date_task = PythonOperator(
        python_callable=check_max_date,
        task_id="check_max_available_date"
    )

    gen_profiles_task = PythonOperator(
        python_callable=generate_customers,
        op_kwargs={"config": config},
        task_id="gen_profiles_table")

    gen_terminals_task = PythonOperator(
        python_callable=generate_terminals,
        op_kwargs={"config": config},
        task_id="gen_terminals_table")

    gen_transactions_task = PythonOperator(
        python_callable=gen_transactions,
        op_kwargs={"config": config},
        task_id="gen_transactions_table")

    add_fraud_task = PythonOperator(
        python_callable=add_fraud,
        op_kwargs={"config": config},
        task_id="add_fraud")

    start_task >> check_date_task

    save_partition = BashOperator(
        task_id='save_partition',
        bash_command="python /home/ubuntu/airflow/dags/scripts/save_partition.py",
    )

    check_date_task >> [gen_profiles_task, gen_terminals_task] >> gen_transactions_task
    gen_transactions_task >> add_fraud_task >> save_partition >> end_task

