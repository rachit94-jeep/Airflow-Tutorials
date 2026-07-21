from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
import pandas as pd
import logging
from pendulum import datetime
log = logging.getLogger(__name__)

# Cron syntax reference:
# ┌─────────── minute       (0–59)
# │ ┌───────── hour         (0–23)
# │ │ ┌─────── day of month (1–31)
# │ │ │ ┌───── month        (1–12)
# │ │ │ │ ┌─── day of week  (0–6, Sun=0)
# │ │ │ │ │
# * * * * *
#
# Special characters:
#   *   any value
#   ,   list separator        e.g. 9,17  → 9 AM and 5 PM
#   -   range                 e.g. 1-5   → Mon through Fri
#   /   step                  e.g. */15  → every 15 units
#
# Common presets (Airflow also accepts these strings directly):
#   @hourly   → 0 * * * *
#   @daily    → 0 0 * * *
#   @weekly   → 0 0 * * 0
#   @monthly  → 0 0 1 * *
#
# This DAG runs at 9 AM and 5 PM every day: '0 9,17 * * *'

@dag(
    dag_id='schedule_cron_dag',
    start_date= datetime(year = 2025, month = 1, day = 1, tz = 'Asia/Kolkata'),
    tags=['ETL', 'schedule'],
    is_paused_upon_creation = False,
    schedule = '0 9,17 * * *',  # 9 AM and 5 PM daily
    catchup = False
)
def schedule_cron_dag():

    @task
    def extract(**kwargs):
        # Push S3 data, Snowflake data, and a weekend flag to XCom for downstream tasks
        ti = kwargs['ti']
        log.info('Extracting data...')
        extracted_data_s3 = [1, 2, 3]
        extracted_data_sf = ['Aman', 'Suresh', 'Mahesh']
        is_weekend = True
        ti.xcom_push(key='extracted_data_s3', value=extracted_data_s3)
        ti.xcom_push(key='extracted_data_sf', value=extracted_data_sf)
        ti.xcom_push(key='is_weekend', value=is_weekend)

    @task
    def transform_s3(**kwargs):
        # Multiply each numeric value by 10
        ti = kwargs['ti']
        pull_s3_data = ti.xcom_pull(task_ids='extract', key='extracted_data_s3')
        log.info(f'Transforming Data....{pull_s3_data}')
        transformed_data = [ele * 10 for ele in pull_s3_data]
        log.info(f'Data Transformation Completed : {transformed_data}')
        ti.xcom_push(key='transformed_data_s3', value=transformed_data)

    @task
    def transform_sf(**kwargs):
        # Uppercase each name from the Snowflake dataset
        ti = kwargs['ti']
        pull_sf_data = ti.xcom_pull(task_ids='extract', key='extracted_data_sf')
        log.info(f'Transforming Data....{pull_sf_data}')
        transformed_data = [ele.upper() for ele in pull_sf_data]
        log.info(f'Data Transformation Completed : {transformed_data}')
        ti.xcom_push(key='transformed_data_sf', value=transformed_data)

    @task(trigger_rule='all_success')
    def createDataframe(**kwargs):
        # Merge S3 (used as index) and Snowflake (names column) data into a single DataFrame
        ti = kwargs['ti']
        read_transformed_data_s3 = ti.xcom_pull(task_ids='transform_s3', key='transformed_data_s3')
        read_transformed_data_sf = ti.xcom_pull(task_ids='transform_sf', key='transformed_data_sf')
        df = pd.DataFrame(data={'name': read_transformed_data_sf}, index=read_transformed_data_s3)
        log.info(df.to_string())

    # Reads the weekend flag and returns the task_id of the branch to follow
    @task.branch(trigger_rule='none_failed_min_one_success')
    def decider_node(**kwargs):
        ti = kwargs['ti']
        is_weekend = ti.xcom_pull(task_ids='extract', key='is_weekend')
        if is_weekend:
            return 'no_load_data'
        else:
            return 'run_this_bash'

    @task
    def no_load_data():
        # Skips the load step on weekends
        log.info("Its weekend!! No Load Day")

    run_this_bash = BashOperator(
        task_id='run_this_bash',
        bash_command='echo "Data is loaded"'
    )

    # Task instances
    first = extract()
    parallel_1 = transform_s3()
    parallel_2 = transform_sf()
    pd_dataframe = createDataframe()
    no_load_day = no_load_data()

    # transform_s3 and transform_sf run in parallel; decider_node routes to load or skip
    first >> [parallel_1, parallel_2] >> pd_dataframe >> decider_node() >> [run_this_bash, no_load_day]

schedule_cron_dag()
