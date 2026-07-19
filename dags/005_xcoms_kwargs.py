from airflow.sdk import dag, task


# DAG demonstrating XCom usage via **kwargs and the TaskInstance (ti) object.
# Unlike the return-value approach, this manually pushes/pulls data through XCom keys.
@dag(
    dag_id = 'xcom_kwargs'
)
def xcomsDag():

    # Extracts raw data and pushes it to XCom manually using the TaskInstance (ti)
    @task.python
    def extract(**kwargs):
        ti = kwargs['ti']  # ti is injected by Airflow into kwargs at runtime
        print('Extracting Data...')
        extracted_data = [1, 2, 3]
        print(kwargs)
        ti.xcom_push(key='extracted_data', value=extracted_data)  # store under a named key

    # Pulls extracted data from XCom, squares each element, and pushes the result
    @task.python
    def transform(**kwargs):
        ti = kwargs['ti']
        print('Transforming Data...')
        extracted_data = ti.xcom_pull(task_ids='extract', key='extracted_data')  # fetch by task_id + key
        transformed_data = [ele**2 for ele in extracted_data]
        ti.xcom_push(key='transformed_data', value=transformed_data)

    # Pulls transformed data from XCom and prints it
    @task.python
    def load(**kwargs):
        ti = kwargs['ti']
        print('Load Data...')
        transformed_data = ti.xcom_pull(task_ids='transform', key='transformed_data')
        print(transformed_data)

    # Register tasks and define execution order via dependency chaining
    first = extract()
    second = transform()
    third = load()

    first >> second >> third

xcomsDag()