# Import the standard Airflow libraries
from airflow.models import DAG
import datetime as dt
from datetime import datetime, date, timedelta
from run_sql_queries import run_query_func
from automated_switchback_test_analysis_script import analysis_script_func
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'oelmaria',
    'email': ['omar.elmaria@deliveryhero.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes = 0.1), # 6 seconds
    'start_date': datetime(2021,1,1)
}

# Define a function that provides the path to the SQL queries and runs the "query_func"
def execute_query_func():
    run_query_func('/opt/airflow/sql_queries/data_extraction_queries_automated_script.sql')

with DAG('switchback_test_dag', schedule_interval = '@daily', default_args = default_args, catchup = False) as dag:
    run_queries = PythonOperator(
        task_id = 'run_queries',
        python_callable = execute_query_func
    )

    success_msg_task_1 = BashOperator(
        task_id = 'success_msg_task_1',
        bash_command = 'echo The "run_queries" task succeeded'
    )

    run_analysis_script = PythonOperator(
        task_id = 'run_analysis_script',
        python_callable = analysis_script_func
    )

    success_msg_task_2 = BashOperator(
        task_id = 'success_msg_task_2',
        bash_command = 'echo The "run_analysis_script" task succeeded'
    )

    # To see how to send emails via Airflow, check these two blog posts --> https://naiveskill.com/send-email-from-airflow/ + https://stackoverflow.com/questions/58736009/email-on-failure-retry-with-airflow-in-docker-container
    success_email_body = f'The switchback testing DAG has been successfully executed at {dt.datetime.now()}'

    send_success_email = EmailOperator(
        task_id = 'send_success_email',
        to = ['<omar.elmaria@deliveryhero.com>'], # The <> are important. Don't forget to include them. You can add more emails to the list
        subject = "The Switchback Testing's Airflow DAG Has Been Successfully Executed",
        html_content = success_email_body,
    )

    # Set the order of tasks
    run_queries >> success_msg_task_1 >> run_analysis_script >> success_msg_task_2 >> send_success_email