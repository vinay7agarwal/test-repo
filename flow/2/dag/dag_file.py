
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator, EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Ensure days_ago is correctly used
    'retries': 1
}

with DAG(
    dag_id='demoflow2',
    default_args=default_args,
    schedule_interval=None
) as dag:
    def log_start_sensor_s3keysensor_8dcb419e2():
        logger.info("Starting task: Sensor-S3KeySensor-8dcb419e2")
 
    def log_end_sensor_s3keysensor_8dcb419e2():
        logger.info("Completed task: Sensor-S3KeySensor-8dcb419e2")
    sensor_s3keysensor_8dcb419e2 = S3KeySensor(
        task_id='Sensor-S3KeySensor-8dcb419e2',
        bucket_name='bh-dag-poc-1',
        bucket_key='job-input/test-airflow.txt',
        aws_conn_id='aws_connection_1',
        on_execute_callback=log_start_sensor_s3keysensor_8dcb419e2,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback
    )

    def log_start_custom_bashoperator_47f84ecb2():
        logger.info("Starting task: Custom-BashOperator-47f84ecb2")
 
    def log_end_custom_bashoperator_47f84ecb2():
        logger.info("Completed task: Custom-BashOperator-47f84ecb2")
    custom_bashoperator_47f84ecb2 = BashOperator(
        task_id='Custom-BashOperator-47f84ecb2',
        bash_command='echo "Test"',
        on_execute_callback=log_start_custom_bashoperator_47f84ecb2,
        on_success_callback=log_end_custom_bashoperator_47f84ecb2
    )

    def log_start_alert_emailoperator_335844370():
        logger.info("Starting task: Alert-EmailOperator-335844370")
 
    def log_end_alert_emailoperator_335844370():
        logger.info("Completed task: Alert-EmailOperator-335844370")
    alert_emailoperator_335844370 = EmailOperator(
        task_id='Alert-EmailOperator-335844370',
        to='vinay@bighammer.ai',
        subject='Demo Project',
        html_content="<h1>test</h1>",
        on_execute_callback=log_start_alert_emailoperator_335844370,
        on_success_callback=log_end_alert_emailoperator_335844370
    )

    def log_start_alert_emailoperator_df884284b():
        logger.info("Starting task: Alert-EmailOperator-df884284b")
 
    def log_end_alert_emailoperator_df884284b():
        logger.info("Completed task: Alert-EmailOperator-df884284b")
    alert_emailoperator_df884284b = EmailOperator(
        task_id='Alert-EmailOperator-df884284b',
        to='vinay@bighammer.ai',
        subject='Demo Project',
        html_content="<h1>Test</h1>",
        on_execute_callback=log_start_alert_emailoperator_df884284b,
        on_success_callback=log_end_alert_emailoperator_df884284b
    )

    # Set task dependencies
    sensor_s3keysensor_8dcb419e2 >> custom_bashoperator_47f84ecb2
