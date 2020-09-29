import json
from datetime import timedelta, datetime
from functools import partial
from pathlib import Path

from airflow import DAG
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
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
    'start_date': days_ago(0),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

DAG_ID = '{p.parent.name}_{p.stem}'.format(p=Path(__file__))
PARAMS = Variable.get(DAG_ID, deserialize_json=True)
DATA_FILE_TYPES = ['.csv', '.json', '.meta', '.schema']

SCHEDULE_INTERVAL = PARAMS.get('schedule_interval') or None

SOURCE_GCS_CONN_ID = PARAMS.get('source_conn_id', 'google_cloud_default')
TARGET_GCS_CONN_ID = PARAMS.get('target_conn_id', 'google_cloud_default')
BQ_CONN_ID = PARAMS.get('bq_conn_id', 'google_cloud_default')
PUBSUB_CONN_ID = PARAMS.get('pubsub_conn_id', 'google_cloud_default')

SOURCE_BUCKET = PARAMS['source_bucket']
SOURCE_PREFIXES = PARAMS.get('source_prefixes')
TARGET_PROJECT = PARAMS['target_project']
TARGET_DATASET = PARAMS['target_dataset']
TARGET_BUCKET = PARAMS.get('target_bucket', SOURCE_BUCKET)
TARGET_PREFIX = PARAMS.get('target_prefix', '')
BACKUP_PREFIX = PARAMS.get('backup_prefix')
TABLE_NAMES = PARAMS['table_names']
NOTIFICATION_PROJECT = PARAMS['notification_project']
NOTIFICATION_TOPIC = PARAMS['notification_topic']


def list_source_files():
    file_prefixes = ('',) if SOURCE_PREFIXES is None else tuple(SOURCE_PREFIXES)
    file_types = ('',) if DATA_FILE_TYPES is None else tuple(DATA_FILE_TYPES)
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=SOURCE_GCS_CONN_ID)
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
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=SOURCE_GCS_CONN_ID)
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
        copied_files.append(destination_object)

    logging.info("copied {} files".format(len(copied_files)))
    return copied_files


def backup_source_files(**context):
    source_files = context['ti'].xcom_pull(dag_id=context['dag'].dag_id.split('.')[0], task_ids='copy_files')
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=SOURCE_GCS_CONN_ID)

    for source_object in source_files:
        if BACKUP_PREFIX is not None:
            # move the source object to 'BACKUP_PREFIX' so that it won't be copied again in the next run.
            new_source_object = BACKUP_PREFIX + source_object
            logging.info("Moving file from gs://{}/{} to gs://{}/{}".format(
                SOURCE_BUCKET, source_object, SOURCE_BUCKET, new_source_object
            ))
            hook.copy(
                source_bucket=SOURCE_BUCKET,
                source_object=source_object,
                destination_object=new_source_object
            )
            hook.delete(
                bucket=SOURCE_BUCKET,
                object=source_object
            )


def get_timestamp(file):
    return file.timestamp


def ingest_file(file, bucket, table, cursor, gcs_hook, write_disposition):
    schema_fields = json.loads(
        gcs_hook.download(bucket, file.schema_file.path).decode("utf-8")
    )
    file_uri = 'gs://{}/{}'.format(bucket, file.path)
    logging.info(
        'Importing file {} to table {} as {}.'.format(file_uri, table, write_disposition)
    )
    if file.partition:
        time_partitioning = {
            'field': TABLE_NAMES[file.table],
            'type': 'DAY'
        }
        destination_table = '{}${}'.format(table, file.partition)
        cursor.run_table_delete(destination_table, ignore_if_missing=True)
    else:
        time_partitioning = {}

    cursor.run_load(
        destination_project_dataset_table=table,
        schema_fields=schema_fields,
        source_uris=[file_uri],
        write_disposition=write_disposition,
        field_delimiter=file.delimiter,
        quote_character=file.quote_character,
        source_format=file.source_format,
        time_partitioning=time_partitioning
    )


def ingest_source_files(table, **context):
    data_files = context['ti'].xcom_pull(dag_id=context['dag'].dag_id.split('.')[0], task_ids='copy_files')
    events = []

    table_files = [
        AbinitioFile.from_path(file, prefix=TARGET_PREFIX)
        for file in data_files
        if file.startswith(TARGET_PREFIX + table)
    ]

    ingest = partial(
        ingest_file,
        bucket=TARGET_BUCKET,
        cursor=BigQueryHook(bigquery_conn_id=BQ_CONN_ID).get_conn().cursor(),
        gcs_hook=GoogleCloudStorageHook(google_cloud_storage_conn_id=TARGET_GCS_CONN_ID)
    )

    destination_table = '.'.join((TARGET_PROJECT, TARGET_DATASET, table))

    partitions = sorted((f for f in table_files if f.partition is not None), key=get_timestamp)
    if partitions:
        for file in partitions:
            ingest(file=file, table=destination_table, write_disposition='WRITE_APPEND')
        events.append(datetime.utcnow().isoformat())

    else:
        fulls = {file.timestamp: file for file in table_files if file.purpose == 'full' and not file.partition}
        latest_full = fulls[max(fulls.keys())] if fulls else None
        deltas = sorted((
            f for f in table_files
            if f.purpose == 'delta' and (latest_full is None or f.timestamp >= latest_full.timestamp)
        ), key=get_timestamp)

        if latest_full:
            ingest(file=latest_full, table=destination_table, write_disposition='WRITE_TRUNCATE')

        for delta in deltas:
            ingest(file=delta, table=destination_table, write_disposition='WRITE_APPEND')

        if latest_full or deltas:
            events.append(datetime.utcnow().isoformat())

    return events


def send_pubsub_message(table, **context):
    updated_tables = context['ti'].xcom_pull(task_ids='ingest_data_{}'.format(table))
    if updated_tables:
        count = len(updated_tables)
        logging.info(
            "Announcing {} change{} to table {}.".format(count, 's' if count > 1 else '', table)
        )
        dataset_id = ":".join((TARGET_PROJECT, TARGET_DATASET))
        messages = [
            {
                "attributes": {
                    "dagId": DAG_ID,
                    "datasetId": dataset_id,
                    "eventTime": event_time + 'Z',
                    "eventType": "TABLE_UPDATE",
                    "skyVersion": "1.0",
                    "tableId": table,
                    "technology": "GCP.BIGQUERY.TABLE"
                }
            } for event_time in updated_tables
        ]
        PubSubHook(gcp_conn_id=PUBSUB_CONN_ID).publish(NOTIFICATION_PROJECT, NOTIFICATION_TOPIC, messages)
    else:
        logging.info("Table {} was not updated.".format(table))


class AbinitioFile:
    __slots__ = ('prefix', 'table', 'country', 'timestamp', 'partition', 'purpose', 'name')
    delimiter = chr(199)
    quote_character = ''

    def __init__(self, prefix='', table=None, country=None, timestamp=None, partition=None, purpose=None, name=None):
        self.prefix = prefix
        self.table = table
        self.country = country
        self.timestamp = timestamp
        self.partition = partition
        self.purpose = purpose
        self.name = name

    def __hash__(self):
        return hash(self.path)

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.path == other.path

    @property
    def ext(self):
        return Path(self.name).suffix

    @property
    def source_format(self):
        return 'NEWLINE_DELIMITED_JSON' if self.ext == '.json' else 'CSV'

    @property
    def parts(self):
        return [self.prefix, self.table, self.country, self.timestamp, self.partition, self.purpose, self.name]

    @property
    def path(self):
        return Path(self.prefix + '/'.join(filter(None, self.parts[1:])))

    def get_gcs_path(self, bucket):
        return 'gs://{}/{}'.format(bucket, self.path)

    @property
    def schema_file(self):
        schema_file = self.from_path(str(self.path.with_suffix('.schema')), prefix=self.prefix)
        schema_file.purpose = 'schema'
        partition_file_part = '_{}_'.format(self.partition)
        if self.partition is not None and partition_file_part in self.name:
            schema_file.partition = None
            schema_file.name = schema_file.name.replace(partition_file_part, '_')
        return schema_file

    @classmethod
    def from_path(cls, path, prefix=''):
        if prefix and path.startswith(prefix):
            path = path[len(prefix):]
        path_parts = [prefix, *path.split('/')]
        # make up for missing partition element in path
        if len(path_parts) == 6:
            path_parts.insert(4, None)
        if len(path_parts) != len(cls.__slots__):
            raise ValueError('The file path {} does not adhere to specifications.'.format(path))
        return cls(**dict(zip(cls.__slots__, path_parts)))


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Implementation of L4 steps 4 and 5.',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1
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

backup_files = PythonOperator(
    dag=dag,
    task_id='backup_source_files',
    python_callable=backup_source_files,
    provide_context=True
)

list_files >> copy_files

for table in sorted(TABLE_NAMES):
    ingest_data = PythonOperator(
        dag=dag,
        task_id='ingest_data_{}'.format(table),
        python_callable=ingest_source_files,
        op_kwargs={'table': table},
        provide_context=True
    )

    announce_ingest = PythonOperator(
        dag=dag,
        task_id='announce_completion_{}'.format(table),
        python_callable=send_pubsub_message,
        op_kwargs={'table': table},
        provide_context=True
    )
    copy_files >> ingest_data >> announce_ingest >> backup_files
