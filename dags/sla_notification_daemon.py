import logging

from airflow import DAG
from util.sla_config import poke_interval
from datetime import datetime, timedelta

from util.sla_monitoring_sensor import DagRunSensorForSlaMonitoring
import pendulum


########################################################################################

def on_failure_callback(context):
    logging.info("inside failure callback")
    dag_id = context["dag_run"].dag_id
    task_id = context["ti"].task_id
    run_id = context["dag_run"].run_id[:-33]  # remove timestamp portion
    execution_date = context["ds"]
    logging.info(str(context['dag_run'].conf))
    logging.info(
        "SLA NOTIFICATION DAEMON ON_FAILURE_CALLBACK PARAMETERS --> dag_id == " + dag_id + " task_id == " + task_id + " run_id == " + run_id)


def on_success_callback(context):
    logging.info("inside success callback")
    dag_id = context["dag_run"].dag_id
    task_id = context["ti"].task_id
    run_id = context["dag_run"].run_id[:-33]  # remove timestamp portion
    execution_date = context["ds"]
    logging.info(str(context['dag_run'].conf))
    logging.info(
        "SLA NOTIFICATION DAEMON ON_SUCCESS_CALLBACK PARAMETERS --> dag_id == " + dag_id + " task_id == " + task_id + " run_id == " + run_id)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 11, tzinfo=pendulum.timezone("US/Eastern")),
    'email': ['saurabh.sinha.ap@nielsen.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_callback,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('sla_notification_daemon', description='sla_notification_daemon', default_args=default_args, catchup=False,
          schedule_interval='06 06 * * *')

dag_run_status = DagRunSensorForSlaMonitoring(
    task_id='dag_run_status',
    dag=dag,
    poke_interval=poke_interval)
