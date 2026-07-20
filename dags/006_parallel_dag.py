from airflow.sdk import dag,task
from airflow.providers.standard.operators.bash import BashOperator
import pandas as pd
import logging

log = logging.getLogger(__name__)

@dag(
    dag_id = 'Parallel_Dag',
    tags = ['ETL','parallel'],
    schedule = None
)
def parallel_dag():

    @task
    def extract(**kwargs):
        ti = kwargs['ti']
        log.info('Extracting data...')
        extracted_data_s3 = [1,2,3]
        extracted_data_sf = ['Aman','Suresh','Mahesh']
        ti.xcom_push(key = 'extracted_data_s3', value = extracted_data_s3)
        ti.xcom_push(key = 'extracted_data_sf',value = extracted_data_sf)

    @task
    def transform_s3(**kwargs):
        ti = kwargs['ti']
        pull_s3_data = ti.xcom_pull(task_ids = 'extract', key = 'extracted_data_s3')
        log.info(f'Transforming Data....{pull_s3_data}')
        transformed_data = [ele * 10 for ele in pull_s3_data]
        log.info(f'Data Transformation Completed : {transformed_data}')
        ti.xcom_push(key = 'transformed_data_s3', value = transformed_data)

    @task
    def transform_sf(**kwargs):
        ti = kwargs['ti']
        pull_sf_data = ti.xcom_pull(task_ids = 'extract', key = 'extracted_data_sf')
        log.info(f'Transforming Data....{pull_sf_data}')
        transformed_data = [ ele.upper() for ele in pull_sf_data]
        log.info(f'Data Transformation Completed : {transformed_data}')
        ti.xcom_push(key = 'transformed_data_sf', value = transformed_data)

    @task(
        trigger_rule = 'none_failed_min_one_success'
    )
    def createDataframe(**kwargs):
        ti = kwargs['ti']
        read_transformed_data_s3 =  ti.xcom_pull(task_ids = 'transform_s3', key = 'transformed_data_s3')
        read_transformed_data_sf =  ti.xcom_pull(task_ids = 'transform_sf', key = 'transformed_data_sf')
        df = pd.DataFrame(data={'name':read_transformed_data_sf},index = read_transformed_data_s3)
        log.info(df.to_string())

    run_this_bash = BashOperator(
        task_id = 'run_this_bash',
        bash_command = 'echo "Data is loaded"'
    )
    
    #Task Instances:
    first = extract()
    parallel_1 = transform_s3()
    parallel_2 = transform_sf()
    pd_dataframe = createDataframe()
    end = run_this_bash

    #Task Chaining
    first >> [parallel_1,parallel_2] >> pd_dataframe >> end

parallel_dag()


    


