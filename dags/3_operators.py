from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag()
def bash_dag():

    @task.python #Python is default if @task is used
    def first_task():
        print('This is the first task')
    
    @task.python
    def second_task():
        print('This is my second task')

    
    @task.bash
    def bash_task_modern() -> str:
        return "echo https://airflow.apache.org/"

    @task.bash
    def check_environment() -> str:
        return "date && whoami && pwd"
    
    
    bash_task_oldschool = BashOperator(
    task_id="bash_task_oldschool",
    bash_command="echo https://airflow.apache.org/",
)

    #Define the dependeicnes 
    first= first_task()
    second = second_task()
    bash_modern = bash_task_modern()
    bash_oldschool = bash_task_oldschool
    check_env = check_environment()
    first >> second >> bash_modern >> bash_oldschool >> check_env

bash_dag()