from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
    start_date=datetime(year=2026, month=7, day=22, tz='Asia/Kolkata'),
    schedule=DeltaTriggerTimetable(duration(days=3)),
    end_date=datetime(year=2026, month=7, day=31, tz='Asia/Kolkata'),
    is_paused_upon_creation=False,
    catchup=True
)
def delta_scheduled_dag():

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

delta_scheduled_dag()