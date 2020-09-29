from datetime import timedelta
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

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
DATA_FILE_TYPES = ['.dat.gz', ]

SCHEDULE_INTERVAL = PARAMS.get('schedule_interval') or None

GCS_CONN_ID = PARAMS.get('gcs_conn_id', 'google_cloud_default')
PUBSUB_CONN_ID = PARAMS.get('pubsub_conn_id', 'google_cloud_default')

SOURCE_BUCKET = PARAMS['source_bucket']
SOURCE_PREFIXES = PARAMS.get('source_prefixes')
TARGET_PROJECT = PARAMS['target_project']
TARGET_BUCKET = PARAMS.get('target_bucket', SOURCE_BUCKET)
TARGET_PREFIX = PARAMS.get('target_prefix', '')
BACKUP_PREFIX = PARAMS.get('backup_prefix')
NOTIFICATION_PROJECT = PARAMS['notification_project']
NOTIFICATION_TOPIC = PARAMS['notification_topic']


def list_source_files():
    file_prefixes = ('',) if SOURCE_PREFIXES is None else tuple(SOURCE_PREFIXES)
    file_types = ('',) if DATA_FILE_TYPES is None else tuple(DATA_FILE_TYPES)
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=GCS_CONN_ID)
    source_files = list(
        filter(
            lambda file: file.startswith(file_prefixes) and file.endswith(file_types),
            hook.list(SOURCE_BUCKET)
        )
    )
    logging.info("Found {} new files to copy".format(len(source_files)))
    return source_files


def copy_source_files(**context):
    source_files = context['ti'].xcom_pull()
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=GCS_CONN_ID)
    copied_files = []
    for source_object in source_files:
        destination_object = TARGET_PREFIX + source_object
        logging.info("Copying file gs://{}/{} to gs://{}/{}".format(
            SOURCE_BUCKET, source_object, TARGET_BUCKET, destination_object
        ))
        hook.copy(
            source_bucket=SOURCE_BUCKET,
            destination_bucket=TARGET_BUCKET,
            source_object=source_object,
            destination_object=destination_object
        )
        copied_files.append((destination_object, datetime.utcnow()))

    logging.info("copied {} files".format(len(copied_files)))
    return copied_files


def move_source_files(**context):
    files_details = context['ti'].xcom_pull(dag_id=context['dag'].dag_id.split('.')[0], task_ids='copy_files')
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=GCS_CONN_ID)

    for file_object, _ in files_details:
        if BACKUP_PREFIX is not None:
            # move the source object to 'BACKUP_PREFIX' so that it won't be copied again in the next run.
            new_source_object = BACKUP_PREFIX + file_object
            logging.info("Moving file from gs://{}/{} to gs://{}/{}".format(
                SOURCE_BUCKET, file_object, SOURCE_BUCKET, new_source_object
            ))
            hook.copy(
                source_bucket=SOURCE_BUCKET,
                source_object=file_object,
                destination_object=new_source_object
            )
            hook.delete(
                bucket=SOURCE_BUCKET,
                object=file_object
            )


def send_pubsub_message(**context):
    files_details = context['ti'].xcom_pull(dag_id=context['dag'].dag_id.split('.')[0], task_ids='copy_files')

    if files_details:
        count = len(files_details)
        logging.info(
            "Announcing {} change{} to bucket {}.".format(count, 's' if count > 1 else '', TARGET_BUCKET)
        )

        messages = [
            {
                "attributes": {
                    "bucketId": TARGET_BUCKET,
                    "eventTime": event_time,
                    "eventType": "OBJECT_FINALIZE",
                    "notificationConfig": "projects/_/buckets/bucket-af/notificationConfigs/12",
                    "objectGeneration": "1528722738995606",
                    "objectId": file_object,
                    "payloadFormat": "JSON_API_V1",
                    "skyVersion": "1.0",
                    "technology": "GCP.CLOUDSTORAGE.OBJECT"
                } for file_object, event_time in files_details
            }
        ]
        PubSubHook(gcp_conn_id=PUBSUB_CONN_ID).publish(NOTIFICATION_PROJECT, NOTIFICATION_TOPIC, messages)
    else:
        logging.info("No files copied into {} bucket to send pubsub message.".format(TARGET_BUCKET))


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Implementation of L4 steps 4 for landmark file move.',
    schedule_interval=SCHEDULE_INTERVAL
)

list_files = PythonOperator(
    dag=dag,
    task_id='list_files',
    python_callable=list_source_files
)

copy_files = PythonOperator(
    dag=dag,
    task_id='copy_files',
    python_callable=copy_source_files,
    provide_context=True
)

move_files = PythonOperator(
    dag=dag,
    task_id='move_files',
    python_callable=move_source_files,
    provide_context=True
)

announce_copy = PythonOperator(
    dag=dag,
    task_id='announce_completion',
    python_callable=send_pubsub_message,
    provide_context=True
)

list_files >> copy_files >> announce_copy >> move_files
