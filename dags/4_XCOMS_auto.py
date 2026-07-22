from airflow.sdk import dag, task

@dag()
def xcoms_dag_auto():

    @task.python #Python is default if @task is used
    def first_task_extract():
        print('Extracting Data')
        fetched_data={"data":[1,2,3,4]}
        return fetched_data
    
    @task.python
    def second_task_transform(data:dict):
        print('Transforming Data')
        fetched_data = data['data']
        transformed_data = fetched_data * 2
        transformed_data_dict = {"transf_data":transformed_data}
        return transformed_data_dict

    
    @task.python
    def third_task_load(data:dict):
        print('Loading Data')
        load_data = data
        return load_data

    #Define the dependeicnes 
    first= first_task_extract()
    second = second_task_transform(first)
    third = third_task_load(second)
    first >> second >> third

xcoms_dag_auto()