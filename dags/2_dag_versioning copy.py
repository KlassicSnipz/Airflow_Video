from airflow.sdk import dag, task

@dag()
def versioned_dag():

    @task.python #Python is default if @task is used
    def first_task():
        print('This is the first task')
    
    @task.python
    def second_task():
        print('This is my second task')

    
    @task.python
    def third_task():
        print('This is my third task, Dag Complete!')


    @task.python
    def version_task():
        print('This is my version task, Dag version 2.0!')

    #Define the dependeicnes 
    first= first_task()
    second = second_task()
    third = third_task()
    version = version_task()
    first >> second >> third >> version

versioned_dag()