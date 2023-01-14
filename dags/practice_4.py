"""
    Даг для формирования признаков.
"""
import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from scripts.tasks import prepare_features

from config import BaseETLConfig
config = BaseETLConfig()

with DAG(
        'practice_4',
        default_args={
            'depends_on_past': False,
            'email': ['slavkotrov@google.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
        },
        description='Features preparation dag for practice_4.',
        schedule_interval=None,
        start_date=datetime.datetime(2022, 7, 30, 0),
        catchup=False,
        tags=['otus_practice'],
) as dag:
    dag.doc_md = __doc__

    start_task = EmptyOperator(task_id="start")

    calc_new_features = PythonOperator(
        python_callable=prepare_features,
        op_kwargs={"config": config},
        task_id="calc_new_features",
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> calc_new_features >> end_task
