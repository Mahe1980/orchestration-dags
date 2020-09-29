from datetime import timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import logging
import re

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

DAG_ID = '{p.parent.name}_{p.stem}'.format(p=Path(__file__))
PARAMS = Variable.get(DAG_ID, deserialize_json=True)

SCHEDULE_INTERVAL = PARAMS.get('schedule_interval') or None

GCS_CONN_ID = PARAMS.get('gcs_conn_id', 'google_cloud_default')
PUBSUB_CONN_ID = PARAMS.get('pubsub_conn_id', 'google_cloud_default')
SOURCE_PREFIXES = tuple(PARAMS.get('source_prefix', []))
DESTINATION_BUCKET = PARAMS['destination_bucket']
DESTINATION_PREFIX = PARAMS.get('destination_prefix', '')
PUBSUB_SUBSCRIPTION = PARAMS['pubsub_subscription']
SOURCE_GCS_CONN_ID = PARAMS.get('source_conn_id', 'google_cloud_default')
SOURCE_BUCKET = PARAMS['source_bucket']

DESTINATION_TRIGGER_FILE_TEMPLATE = Path(os.getenv('AIRFLOW_HOME')) / 'empty_file.ok'
if not DESTINATION_TRIGGER_FILE_TEMPLATE.exists():
    DESTINATION_TRIGGER_FILE_TEMPLATE.write_text('')


def get_new_files():
    file_prefixes = ('',) if SOURCE_PREFIXES is None else tuple(SOURCE_PREFIXES)
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=SOURCE_GCS_CONN_ID)
    source_files = list(
        filter(
            lambda file: file.startswith(file_prefixes) and re.search('20190507', file), hook.list(SOURCE_BUCKET)
        )
    )
    logging.info("Found {} new files to copy".format(len(source_files)))
    return source_files


def copy_listed_files(**context):
    source_files = context['ti'].xcom_pull()
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=SOURCE_GCS_CONN_ID)
    copied_files = []
    trigger_file = str(DESTINATION_TRIGGER_FILE_TEMPLATE)
    for source_object in source_files:
        destination_object = DESTINATION_PREFIX + source_object
        logging.info("Copying file gs://{}/{} to gs://{}/{}".format(
            SOURCE_BUCKET, source_object, DESTINATION_BUCKET, destination_object
        ))
        hook.copy(
            source_bucket=SOURCE_BUCKET,
            destination_bucket=DESTINATION_BUCKET,
            source_object=source_object,
            destination_object=destination_object
        )
        copied_files.append(destination_object)

        hook.upload(DESTINATION_BUCKET, destination_object + '_OK', trigger_file)

    logging.info("copied {} files".format(len(copied_files)))
    return copied_files


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Implementation of L4 step 2.',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1
)

list_new_files = PythonOperator(
    dag=dag,
    task_id='list_new_files',
    python_callable=get_new_files
)

copy_files = PythonOperator(
    dag=dag,
    task_id='copy_files',
    python_callable=copy_listed_files,
    provide_context=True
)

dag >> list_new_files >> copy_files