from airflow.sdk import dag, task, get_current_context
import logging
from pendulum import datetime
from datetime import timedelta

log = logging.getLogger(__name__)

# Incremental Loading with timedelta scheduling:
#   Each DAG run is assigned a data window:
#     data_interval_start → data_interval_end
#   Tasks use this window to fetch ONLY the data for that period.
#   This avoids re-processing old data on every run (full load).
#
#   Example with schedule=timedelta(days=1):
#     Run 1: window [2025-01-01 00:00 → 2025-01-02 00:00]
#     Run 2: window [2025-01-02 00:00 → 2025-01-03 00:00]
#     Run 3: window [2025-01-03 00:00 → 2025-01-04 00:00]
#
#   Windows are non-overlapping and contiguous — no duplicate or missing data.
#
#   catchup=True:
#     If the DAG was paused or newly deployed, Airflow backfills all missed
#     windows in order. Essential for incremental ETL — ensures no data gaps.
#
#   catchup=False:
#     Missed windows are skipped. Fine for dashboards/reports, bad for ETL
#     where every window of data must be loaded.
#
#   Manual trigger note:
#     When triggered manually, Airflow sets data_interval_start == data_interval_end
#     (zero-length window). Use get_current_context() to read the interval; in
#     production always rely on scheduled runs for correct windows.

@dag(
    dag_id='incremental_load_dag_2',
    start_date=datetime(2026, 7, 13, tz='Asia/Kolkata'),
    tags=['ETL', 'incremental'],
    is_paused_upon_creation=False,
    schedule=timedelta(days=1),  # 1-day windows; change to timedelta(hours=1) for hourly loads
    catchup=True  # backfill missed windows — required for incremental ETL
)
def incremental_load_dag2():

    @task
    def extract():
        ctx   = get_current_context()
        start = ctx['data_interval_start']
        end   = ctx['data_interval_end']
        log.info(f'Extracting data for window: {start} → {end}')

        # In a real pipeline, pass start/end to your data source query:
        #   SELECT * FROM orders
        #   WHERE created_at >= '{start}' AND created_at < '{end}'
        extracted_data = [
            {'id': 1, 'amount': 100, 'created_at': str(start)},
            {'id': 2, 'amount': 200, 'created_at': str(start)},
        ]
        log.info(f'Extracted {len(extracted_data)} records')
        return extracted_data

    @task
    def transform(extracted_data: list):
        transformed = [
            {**record, 'amount_usd': record['amount'] * 0.012}
            for record in extracted_data
        ]
        log.info(f'Transformed {len(transformed)} records')
        return transformed

    @task
    def load(transformed_data: list):
        ctx   = get_current_context()
        start = ctx['data_interval_start']
        end   = ctx['data_interval_end']

        # In a real pipeline, append (INSERT) these records into your warehouse.
        # Never do a full overwrite here — that defeats incremental loading.
        #   INSERT INTO orders_warehouse SELECT * FROM staging WHERE ...
        log.info(f'Loading {len(transformed_data)} records for window {start} → {end}')
        log.info('Load complete — records appended to warehouse')

    # Linear pipeline: extract → transform → load
    raw = extract()
    cleaned = transform(raw)
    load(cleaned)

incremental_load_dag2()
