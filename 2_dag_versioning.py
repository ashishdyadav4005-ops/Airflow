from airflow.sdk import dag, task

@dag(
    dag_id="versioned_dag",
)
def versioned_dag():

    @task.python
    def first_task():
        print("this is the first task")

    @task.python
    def second_task():
        print("this is the second task")

    @task.python
    def third_task():
        print("this is the third task. DAG completed")

    first=first_task()
    second=second_task()
    third=third_task()

    first>>second>>third

versioned_dag()