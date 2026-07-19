from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id='operators_dag',
    schedule=None,
)
def operators_dag():

    create_file = BashOperator(
        task_id='create_file',
        bash_command='touch /opt/airflow/dags/004_xcoms.py && echo "File created!"',
    )

    create_file


operators_dag()
