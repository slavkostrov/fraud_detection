"""
    Даг для обучения модели.
"""
import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from scripts.model_train import train

from config import BaseETLConfig
config = BaseETLConfig()

with DAG(
        'practice_5',
        default_args={
            'depends_on_past': False,
            'email': ['slavkotrov@google.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
        },
        description='Model fit dag for practice_5.',
        schedule_interval="@weekly",
        start_date=datetime.datetime(2022, 9, 11, 0),
        catchup=False,
        tags=['otus_practice'],
) as dag:
    dag.doc_md = __doc__

    start_task = EmptyOperator(task_id="start")

    train_eval_model = PythonOperator(
        python_callable=train,
        op_kwargs={"config": config},
        task_id="train_model",
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> train_eval_model >> end_task
