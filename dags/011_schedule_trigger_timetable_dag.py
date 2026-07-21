from airflow.sdk import dag, task
from airflow.timetables.trigger import DeltaTriggerTimetable
import logging
from pendulum import datetime
from datetime import timedelta

log = logging.getLogger(__name__)

# DeltaTriggerTimetable (Airflow 2.9+):
#   An explicit timetable that fires every fixed timedelta interval.
#   Unlike plain timedelta (DeltaDataIntervalTimetable), it does NOT create
#   a data interval — logical_date equals the actual trigger time.
#
#   Use it when:
#     - tasks don't need data_interval_start / data_interval_end
#     - you want logical_date to reflect when the DAG ran, not a window boundary
#
#   Comparison:
#     schedule = timedelta(days=3)
#       → logical_date = data_interval_start (start of the 3-day window)
#
#     schedule = DeltaTriggerTimetable(timedelta(days=3))
#       → logical_date = trigger time (no window concept)
#
#   The start_date time component anchors the first run time.
#   Here: first run on 2025-01-01 at 9 AM, then every 3 days at 9 AM.

@dag(
    dag_id='schedule_trigger_timetable_dag',
    start_date=datetime(2025, 1, 1, 9, 0, 0, tz='Asia/Kolkata'),  # anchors run at 9 AM
    tags=['ETL', 'schedule'],
    is_paused_upon_creation=False,
    schedule=DeltaTriggerTimetable(timedelta(days=3)),  # every 3 days, logical_date = trigger time
    catchup=False
)
def schedule_trigger_timetable_dag():

    @task
    def print_trigger_time(**kwargs):
        # logical_date is the actual trigger time (not a window boundary)
        logical_date = kwargs['logical_date']
        log.info(f'DAG triggered at: {logical_date}')

    print_trigger_time()

schedule_trigger_timetable_dag()
