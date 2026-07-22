from airflow.sdk import dag, task

@dag()
def branch_dag():

    @task.python
    def extract_task(**kwargs):
        print('Extracting Data ....')
        ti = kwargs['ti']
        extracted_data_dict = {
            'api_extracted_data': [1, 2, 3],
            'db_extracted_data': [4, 5, 6],
            's3_extracted_data': [7, 8, 9],
            'weekend_flag': 'false'
        }
        ti.xcom_push(key='return_value', value=extracted_data_dict)

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids='extract_task', key='return_value')['api_extracted_data']
        print(f"Transforming API data: {api_extracted_data}.....")
        transformed_api_data = [i * 10 for i in api_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_api_data)

    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids='extract_task', key='return_value')['db_extracted_data']
        print(f'Transforming DB data: {db_extracted_data}......')
        transformed_db_data = [i * 10 for i in db_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_db_data)

    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(task_ids='extract_task', key='return_value')['s3_extracted_data']
        print(f'Transforming S3 data: {s3_extracted_data}......')
        transformed_s3_data = [i * 10 for i in s3_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_s3_data)
    
    
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(task_ids='extract_task', key='return_value')['weekend_flag']
        if weekend_flag == 'true':
            return 'no_load_task'
        else:
            return 'load_task'
    
    
    @task.bash
    def load_task(**kwargs):
        ti = kwargs['ti']
        api_data = ti.xcom_pull(task_ids='transform_task_api', key='return_value')
        db_data = ti.xcom_pull(task_ids='transform_task_db', key='return_value')
        s3_data = ti.xcom_pull(task_ids='transform_task_s3', key='return_value')
        return f"echo Loaded data - API: {api_data} DB: {db_data} S3: {s3_data}"

   

    @task.bash
    def no_load_task(**kwargs):
        print("No Load Task Executed")
        return "echo 'No Load Task Executed'"

    # Define the dependencies
    extract = extract_task()
    api = transform_task_api()
    db = transform_task_db()
    s3 = transform_task_s3()
    load = load_task()
    decider = decider_task()
    no_load = no_load_task()

    extract >> [api, db, s3] >> decider >> [load, no_load]

branch_dag()