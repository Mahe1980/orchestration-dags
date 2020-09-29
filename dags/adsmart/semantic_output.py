from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import Variable
from airflow.utils.dates import days_ago

from common import pubsub_pull_messages

import logging

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

DAG_ID = '{p.parent.name}_{p.stem}'.format(p=Path(__file__))
PARAMS = Variable.get(DAG_ID, deserialize_json=True)
SCHEDULE_INTERVAL = PARAMS.get('schedule_interval') or None
GCS_CONN_ID = PARAMS.get('gcs_conn_id', 'google_cloud_default')
PUBSUB_SUBSCRIPTION = PARAMS['pubsub_subscription']
PUBSUB_CONN_ID = PARAMS.get('pubsub_conn_id', 'google_cloud_default')
SOURCE_PREFIXES = tuple(PARAMS.get('source_prefix', []))
DATA_FILE_TYPES = tuple(PARAMS.get("data_file_type", []))
DESTINATION_PREFIX = tuple(PARAMS.get('destination_prefix', []))
NOTIFICATION_PROJECT = PARAMS['notification_project']
NOTIFICATION_TOPIC = PARAMS['notification_topic']
TARGET_BUCKET = PARAMS.get('target_bucket')


def get_updated_files(subscription):
    pubsub_hook = PubSubHook(gcp_conn_id=PUBSUB_CONN_ID)
    files = {}
    for message in pubsub_pull_messages(subscription, pubsub_hook):
        path = message['message']['attributes']['objectId']
        if path.startswith(SOURCE_PREFIXES) and path.endswith(DATA_FILE_TYPES):
            if path not in files:
                bucket = message['message']['attributes']['bucketId']
                logging.info("New file to copy: gs://{}/{}".format(bucket, path))
                files[path] = {
                    'bucket': bucket,
                    'ack_ids': set()
                }
            files[path]['ack_ids'].add(message['ackId'])
    logging.info("Found {} files to copy.".format(len(files)))
    return files


def copy_updated_files(**context):
    updated_files = context['ti'].xcom_pull()
    gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=GCS_CONN_ID)
    events = {}

    for source_object, details in updated_files.items():
        source_bucket = details['bucket']
        destination_object = DESTINATION_PREFIX + source_object
        logging.info("Copying file gs://{}/{} to gs://{}/{}".format(
            source_bucket, source_object, TARGET_BUCKET, destination_object
        ))
        gcs_hook.copy(
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=TARGET_BUCKET,
            destination_object=destination_object
        )
        events[destination_object] = datetime.utcnow()
    return events


def send_pubsub_message(**context):
    events = context['ti'].xcom_pull(dag_id=context['dag'].dag_id.split('.')[0], task_ids='transfer_data_to_repo')

    if events:
        logging.info(
            "Announcement of copy files in bucket {}.".format(TARGET_BUCKET)
        )
        messages = [
            {
                "attributes": {
                    "bucketId": TARGET_BUCKET,
                    "eventTime": event.isoformat() + 'Z',
                    "eventType": "OBJECT_FINALIZE",
                    "notificationConfig": "projects/_/buckets/bucket-af/notificationConfigs/12",
                    "objectGeneration": "1528722738995606",
                    "objectId": object,
                    "payloadFormat": "JSON_API_V1",
                    "skyVersion": "1.0",
                    "technology": "GCP.CLOUDSTORAGE.OBJECT"
                }
            } for object, event in events
        ]
        PubSubHook(gcp_conn_id=PUBSUB_CONN_ID).publish(NOTIFICATION_PROJECT, NOTIFICATION_TOPIC, messages)
    else:
        logging.info("Files not copied in {} bucket.".format(TARGET_BUCKET))


def acknowledge_copied_files(subscription, **context):
    listed_files = context['ti'].xcom_pull()
    gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=GCS_CONN_ID)
    pubsub_hook = PubSubHook(gcp_conn_id=PUBSUB_CONN_ID)
    project, subscription = subscription.split('.')

    for source_object, details in listed_files.items():
        ack_ids = details['ack_ids']
        destination_object = DESTINATION_PREFIX + source_object
        destination_path = "gs://{}/{}".format(TARGET_BUCKET, destination_object)
        if gcs_hook.exists(TARGET_BUCKET, destination_object):
            if ack_ids:
                logging.info("Acknowledging {} messages for {}".format(len(ack_ids), destination_path))
                pubsub_hook.acknowledge(project, subscription, list(ack_ids))
        else:
            logging.warning("File {} not copied in time".format(destination_path))
    # return empty dict to clear the XCom
    return {}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Implementation of L4 steps 8.',
    schedule_interval=SCHEDULE_INTERVAL
)

list_files = PythonOperator(
    dag=dag,
    task_id='list_files',
    python_callable=get_updated_files,
    op_kwargs={'subscription': PUBSUB_SUBSCRIPTION}
)

copy_files = PythonOperator(
    dag=dag,
    task_id='copy_files',
    python_callable=copy_updated_files,
    provide_context=True
)

announce_copied_files = PythonOperator(
    dag=dag,
    task_id='announce_copied_files',
    python_callable=send_pubsub_message,
    provide_context=True
)

acknowledge_copied = PythonOperator(
    dag=dag,
    task_id='acknowledge_copied',
    python_callable=acknowledge_copied_files,
    op_kwargs={'subscription': PUBSUB_SUBSCRIPTION},
    provide_context=True
)

dag >> list_files >> copy_files >> announce_copied_files >> acknowledge_copied
