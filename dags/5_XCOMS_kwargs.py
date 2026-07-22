from airflow.sdk import dag, task

@dag()
def xcoms_dag_manual():

    @task.python #Python is default if @task is used
    def first_task_extract(**state):
        #Extracting ti from kwargs to push XComs manually
        ti = state['ti']
        print('Extracting Data')
        fetched_data={"data":[1,2,3,4]}
        # Connect it to the return key
        ti.xcom_push(key='return_value', value=fetched_data)
        #return fetched_data
    
    @task.python
    def second_task_transform(**state):
        ti = state['ti'] 
        #Pulling Xcoms pushed by the first task
        fetched_data = ti.xcom_pull(task_ids='first_task', key = 'return_result')
        print('Transforming Data')
        data_list = fetched_data['data']
        transformed_data = fetched_data * 2
        transformed_data_dict = {"transf_data":transformed_data}
        ti.xcom_push(key='return_result', value=transformed_data_dict)
        #return transformed_data_dict

    
    @task.python
    def third_task_load(**state):
        ti=state['ti']
        load_data = ti.xcom_pull(task_ids='second task', ket = 'return_result')
        # print('Loading Data')
        # load_data = data
        return load_data

    #Define the dependeicnes 
    first= first_task_extract()
    second = second_task_transform()
    third = third_task_load()
    first >> second >> third

xcoms_dag_manual()