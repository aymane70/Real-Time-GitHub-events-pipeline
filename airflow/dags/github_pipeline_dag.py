from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='github_events_pipeline',
    default_args=default_args,
    description='Continuous GitHub events pipeline',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    init_db_task = BashOperator(
        task_id='init_database',
        bash_command='python3 /opt/airflow/dags/init_db.py'
    )

    github_fetch_task = BashOperator(
        task_id='fetch_github_events',
        bash_command='python3 /opt/airflow/dags/github_producer.py'
    )

    spark_consumer_task = BashOperator(
        task_id='spark_consumer',
        bash_command='python3 /opt/airflow/dags/spark_consumer.py'
    )

    # Order: init DB first, then the two continuous tasks in parallel
    init_db_task >> [github_fetch_task, spark_consumer_task]
