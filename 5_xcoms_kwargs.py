from airflow.sdk import dag, task

@dag(
    dag_id="xcoms_dag_kwargs"
)
def xcoms_dag_kwargs():

    @task.python
    def first_task(**kwargs):
        t1=kwargs['ti']
        print("xtracting data...this is the first task")
        fetched_data={"data":[1,2,3,4,5]}
        t1.xcom_push(key='return_return',value=fetched_data)

    @task.python
    def second_task(**kwargs):
        t1=kwargs['ti']

        fetched_data=t1.xcom_pull(task_ids='first_task',key='return_return')['data']
        print("transforming data...this is the second task")
        transformed_data=fetched_data*2
        transformed_data_dict={"transf_data":transformed_data}
        t1.xcom_push(key='return_return',value=transformed_data_dict)

    @task.python
    def third_task(**kwargs):
        t1=kwargs['ti']
        load_data=t1.xcom_pull(task_ids='second_task',key='return_return')
        return load_data
    
    first=first_task()
    second=second_task()
    third=third_task()
 
    first>>second>>third

xcoms_dag_kwargs()
                                  