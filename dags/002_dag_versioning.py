from airflow.sdk import dag,task

@dag(
    dag_id = 'dag_versioning',
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
        print('Third Task')
    
    @task.bash
    def bash_task():
        return r"touch dags/003_opearator.py"
    
    first = my_first_task()
    second = my_second_task()
    third = my_third_task()
    bash = bash_task()

    #Creating dependencies
    first >> second >> third >> bash


#Registering dag

my_first_dag()