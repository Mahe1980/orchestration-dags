from datetime import timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from multiprocessing import Pool
from functools import partial

import logging

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

DESTINATION_TRIGGER_FILE_TEMPLATE = Path(os.getenv('AIRFLOW_HOME')) / 'empty_file.ok'
if not DESTINATION_TRIGGER_FILE_TEMPLATE.exists():
    DESTINATION_TRIGGER_FILE_TEMPLATE.write_text('')


def pubsub_pull_messages(subscription, hook, max_messages=100):
    project, subscription = subscription.split('.')
    while True:
        messages = hook.pull(project, subscription, max_messages, return_immediately=True)
        if messages:
            yield from iter(messages)
        else:
            raise StopIteration


def get_new_files(subscription):
    pubsub_hook = PubSubHook(gcp_conn_id=PUBSUB_CONN_ID)
    files = {}
    ack_ids = set()
    for message in pubsub_pull_messages(subscription, pubsub_hook):
        path = message['message']['attributes']['objectId']
        if path.startswith(SOURCE_PREFIXES):
            if path not in files:
                bucket = message['message']['attributes']['bucketId']
                logging.info("New file to copy: gs://{}/{}".format(bucket, path))
                files[path] = {
                    'bucket': bucket,
                    'created': message['message']['attributes']['eventTime'],
                    'ack_ids': set()
                }
            files[path]['ack_ids'].add(message['ackId'])
        else:
            ack_ids.add(message['ackId'])

    # ack messages that are not relevant to this project
    if ack_ids:
        sub_project, sub = subscription.split('.')
        logging.info("Acknowledging {} messages that are not relevant to this project.".format(len(ack_ids)))
        ack_ids = list(ack_ids)
        chunks = [ack_ids[x:x + 1000] for x in range(0, len(ack_ids), 1000)]
        for chunk in chunks:
            pubsub_hook.acknowledge(sub_project, sub, chunk)

    logging.info("Found {} files to copy.".format(len(files)))
    return files


def copy_ack_parallel(project, subscription, trigger_file, details):
    gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=GCS_CONN_ID)
    pubsub_hook = PubSubHook(gcp_conn_id=PUBSUB_CONN_ID)

    source_object = details[0]
    source_bucket = details[1]['bucket']
    destination_object = DESTINATION_PREFIX + source_object
    if not gcs_hook.exists(DESTINATION_BUCKET, destination_object):
        gcs_hook.copy(
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=destination_object
        )

    ok_file = destination_object + '_OK'
    if not gcs_hook.exists(DESTINATION_BUCKET, ok_file):
        gcs_hook.upload(DESTINATION_BUCKET, destination_object + '_OK', trigger_file)

    ack_ids = details[1]['ack_ids']
    if ack_ids:
        pubsub_hook.acknowledge(project, subscription, list(ack_ids))

    logging.info("Copied/acknowledged/saved OK for {}".format(source_object))


def copy_ack(subscription, **context):
    listed_files = context['ti'].xcom_pull()
    listed_files = [(k, v) for k, v in listed_files.items()]
    project, subscription = subscription.split('.')
    trigger_file = str(DESTINATION_TRIGGER_FILE_TEMPLATE)

    with Pool(processes=10) as pool:
        pool.map(partial(copy_ack_parallel, project, subscription, trigger_file), listed_files)

    if len(listed_files) > 0:
        logging.info("Copied {} files successfully".format(len(listed_files)))
    else:
        logging.info("No files to copy")


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Implementation of L4 step 2.',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20)
)

list_new_files = PythonOperator(
    dag=dag,
    task_id='list_new_files',
    python_callable=get_new_files,
    op_kwargs={'subscription': PUBSUB_SUBSCRIPTION}
)

copy_ack = PythonOperator(
    dag=dag,
    task_id='copy_ack',
    python_callable=copy_ack,
    op_kwargs={'subscription': PUBSUB_SUBSCRIPTION},
    provide_context=True
)


dag >> list_new_files >> copy_ack
