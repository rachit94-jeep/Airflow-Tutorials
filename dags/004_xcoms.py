from airflow.sdk import dag,task


@dag(
    dag_id = 'xcoms_dag'
)
def xcomsDag():
    
    @task.python
    def extract():
        print('Extracting Data...')
        extracted_data  = [1,2,3]
        return extracted_data
    
    @task.python
    def transform(raw_data):
        print('Transforming Data...')
        transformed_data = [ele**2 for ele in raw_data]
        return transformed_data

    @task.python
    def load(transformed_data):
        print('Load Data...')
        print(transformed_data)
    
    #Registering Tasks
    first = extract()
    second = transform(first)
    third = load(second)

xcomsDag()