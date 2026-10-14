from airflow.decorators import task, dag
from datetime import datetime
from producer import get_status

now = datetime.now()


@dag(
    dag_id="health_check",
    start_date=now,
    schedule="@hourly",
    catchup=False,
)
def health_check():

    @task.bash
    def check_producer():
        if get_status() == 200:
            return ""
        else:
            return "echo 'Error: Check the logs.'"

    check_producer()


health_check()