from airflow.sdk import dag,task

@dag(
    dag_id = 'first_dag',
    schedule = None
)
def my_first_dag():

    @task.python
    def my_first_task():
        print('First Task')
    
    @task.python
    def my_second_task():
        print('Second Task')
    
    @task.python
    def my_third_task():
        print('Thrid Task')
    
    first = my_first_task()
    second = my_second_task()
    third = my_third_task()

    #Creating dependencies
    first >> second >> third

#Registering dag

my_first_dag()