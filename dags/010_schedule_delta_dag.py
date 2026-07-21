from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
import pandas as pd
import logging
from pendulum import datetime
from datetime import timedelta
log = logging.getLogger(__name__)

# Delta / timedelta scheduling:
#   Pass a timedelta object to `schedule` instead of a cron string.
#   The DAG fires every fixed interval measured from start_date (or the
#   last successful run), regardless of wall-clock time.
#
#   Examples:
#     timedelta(minutes=30)      → every 30 minutes
#     timedelta(hours=6)         → every 6 hours
#     timedelta(days=1)          → every 24 hours (same as @daily but interval-based)
#     timedelta(days=7)          → every week
#
#   vs cron:
#     cron '0 9 * * *'  → fires at exactly 9 AM every day (wall-clock)
#     timedelta(hours=24) → fires 24 h after the previous run completed
#
#   Use delta when you care about the *gap between runs*, not the *time of day*.
#
#   Running every N days AT a specific time:
#     Combine timedelta(days=N) with a start_date that includes the desired time.
#     Airflow uses start_date as the anchor — each subsequent run fires exactly
#     N days later at the same time-of-day offset.
#
#     Example — every 3 days at 9 AM:
#       start_date = datetime(2025, 1, 1, 9, 0, 0, tz='Asia/Kolkata')
#       schedule   = timedelta(days=3)
#
#     vs cron '0 9 */3 * *':
#       */3 means days 1,4,7,...,31 of the month — resets each month so the
#       gap around month boundaries may be less than 3 days.
#       timedelta(days=3) guarantees a true 3-day gap regardless of month boundary.

@dag(
    dag_id='schedule_delta_dag',
    start_date= datetime(year = 2025, month = 1, day = 1, tz = 'Asia/Kolkata'),
    tags=['ETL', 'schedule'],
    is_paused_upon_creation = False,
    schedule = timedelta(hours=8),  # run every 8 hours from start_date
    catchup = False
)
def schedule_delta_dag():

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

schedule_delta_dag()
