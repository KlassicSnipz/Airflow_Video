from airflow.sdk import dag, task

@dag(
    dag_id='first_dag'
)
def first_dag():

    @task.python #Python is default if @task is used
    def first_task():
        print('This is the first task')
    
    @task.python
    def second_task():
        print('This is my second task')

    
    @task.python
    def third_task():
        print('This is my third task, Dag Complete!')

    #Define the dependeicnes 
    first= first_task()
    second = second_task()
    third = third_task()
    first >> second >> third

first_dag()