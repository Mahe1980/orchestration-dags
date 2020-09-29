from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from google.cloud import storage
from airflow import AirflowException
import json
import logging
from multiprocessing import Pool
from functools import partial


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
NOTIFICATION_PROJECT = PARAMS['notification_project']
NOTIFICATION_TOPIC = PARAMS['notification_topic']
TABLE = DAG_ID.split('_abinitio_output_')[1]
PARTITION_FIELD = PARAMS.get('partition_field', '')
KEY_PATH = PARAMS['key_path']


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
    if len(source_files) == 0:
        raise AirflowException('Failing DAG as no files to process!')

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


def move_source_files(**context):
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


def send_pubsub_message(**context):
    updated_tables = context['ti'].xcom_pull(task_ids='ingest_data')
    if updated_tables:
        count = len(updated_tables)
        logging.info(
            "Announcing {} change{} to table {}.".format(count, 's' if count > 1 else '', TABLE)
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
                    "tableId": TABLE,
                    "technology": "GCP.BIGQUERY.TABLE"
                }
            } for event_time in updated_tables
        ]
        PubSubHook(gcp_conn_id=PUBSUB_CONN_ID).publish(NOTIFICATION_PROJECT, NOTIFICATION_TOPIC, messages)
    else:
        logging.info("Table {} was not updated.".format(TABLE))


def get_timestamp(file):
    return file.timestamp


def parse_schema_field(field):
    """Parse a single schema field from json.
    Args:
      field: Single schema json element
    Returns:
      A TableFieldSchema for a single column in BigQuery.
    """

    name = field['name']
    field_type = field['type']
    description = None
    fields = []

    if 'mode' in field:
        mode = field['mode']
    else:
        mode = 'NULLABLE'

    if 'description' in field:
        description = field['description']
    if 'fields' in field:
        fields = [parse_schema_field(x) for x in field['fields']]

    if len(fields) > 0:
        return bigquery.SchemaField(name=name, field_type=field_type, mode=mode, description=description, fields=fields)
    else:
        return bigquery.SchemaField(name=name, field_type=field_type, mode=mode, description=description)


def ingest_file(table, write_disposition, file):
    storage_client = storage.Client.from_service_account_json(project=TARGET_PROJECT, json_credentials_path=KEY_PATH)
    bucket = storage_client.get_bucket(TARGET_BUCKET)
    blob = bucket.blob(str(file.schema_file.path))

    schema_fields = json.loads(blob.download_as_string())

    start_time = datetime.now().replace(microsecond=0)

    bigquery_client = bigquery.Client.from_service_account_json(project=TARGET_PROJECT, json_credentials_path=KEY_PATH)
    dataset_ref = bigquery_client.dataset(TARGET_DATASET)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [parse_schema_field(f) for f in schema_fields]

    if file.source_format == 'CSV':
        job_config.field_delimiter = file.delimiter
        job_config.quote_character = file.quote_character

    job_config.source_format = file.source_format
    job_config.write_disposition = write_disposition
    job_config.create_disposition = 'CREATE_IF_NEEDED'

    if file.partition:
        time_partitioning = bigquery.table.TimePartitioning(type_='DAY', field=PARTITION_FIELD)
        destination_table = '{}${}'.format(table, file.partition)
        bigquery_client.delete_table(destination_table)
    else:
        time_partitioning = None
        destination_table = table

    job_config.time_partitioning = time_partitioning
    file_uri = 'gs://{}/{}'.format(TARGET_BUCKET, file.path)

    load_job = bigquery_client.load_table_from_uri(file_uri, dataset_ref.table(TABLE), location='EU',
                                                   job_config=job_config)  # API request
    load_job.result()  # Waits for table load to complete.

    job_duration = (datetime.now().replace(microsecond=0) - start_time)

    gb = float(load_job.output_bytes) / (1024 * 1024 * 1024)
    bytes_gb = str("{0:.4f}".format(gb))

    # Log job summary
    logging.info("Tag:{tag}|Project:{project}|Source:{source_file}|Destination:{destination_table}|"
                 "OutputRows:{output_rows}|OutputBytes:{output_bytes}B|OutputGB:{output_gb}GB|JobType:{job_type}|"
                 "Duration:{job_duration}".format(tag='ingest_data_from_file_to_table', source_file=file_uri,
                                                  destination_table=destination_table, project=load_job.project,
                                                  output_rows=load_job.output_rows, output_bytes=load_job.output_bytes,
                                                  output_gb=bytes_gb, job_type=load_job.job_type,
                                                  job_duration=job_duration))

    return load_job.output_bytes, load_job.output_rows, job_duration


def ingest_source_files(**context):
    data_files = context['ti'].xcom_pull(dag_id=context['dag'].dag_id.split('.')[0], task_ids='copy_files')
    events = []

    table_files = [
        AbinitioFile.from_path(file, prefix=TARGET_PREFIX)
        for file in data_files
        if file.startswith(TARGET_PREFIX + TABLE)
    ]

    destination_table = '.'.join((TARGET_PROJECT, TARGET_DATASET, TABLE))

    partitions = sorted((f for f in table_files if f.partition is not None), key=get_timestamp)
    total_bytes = 0
    total_rows = 0
    total_duration = timedelta(seconds=0)
    is_latest_full = False

    if partitions:
        start_time = datetime.now().replace(microsecond=0)
        with Pool(processes=10) as pool:
            results = pool.map(partial(ingest_file, destination_table, 'WRITE_APPEND'), partitions)

        for output in results:
            total_bytes += output[0]
            total_rows += output[1]

        total_duration = (datetime.now().replace(microsecond=0) - start_time)
        events.append(datetime.utcnow().isoformat())
    else:
        fulls = {file.timestamp: file for file in table_files if file.purpose == 'full' and not file.partition}
        latest_full = fulls[max(fulls.keys())] if fulls else None
        deltas = sorted((
            f for f in table_files
            if f.purpose == 'delta' and (latest_full is None or f.timestamp >= latest_full.timestamp)
        ), key=get_timestamp)

        if latest_full:
            output_bytes, output_rows, output_duration = ingest_file(table=destination_table,
                                                                     write_disposition='WRITE_TRUNCATE',
                                                                     file=latest_full)
            total_rows = output_rows
            total_bytes = output_bytes
            total_duration = output_duration
            is_latest_full = True

        for delta in deltas:
            output_bytes, output_rows, output_duration = ingest_file(table=destination_table,
                                                                     write_disposition='WRITE_APPEND',
                                                                     file=delta)
            total_bytes += output_bytes
            total_rows += output_rows
            total_duration += output_duration

        if latest_full or deltas:
            events.append(datetime.utcnow().isoformat())

    if is_latest_full is False:
        # Log number of bytes processed
        gb = float(total_bytes) / (1024 * 1024 * 1024)
        gb = str("{0:.4f}".format(gb))
        logging.info("Tag:{tag}|TotalRows:{total_rows}|Destination|{destination_table}|TotalBytes:{total_bytes}B|"
                     "TotalGB:{total_gb}GB|TotalDuration:{total_duration}".
                     format(tag='ingest_data_from_file_to_table', destination_table=destination_table,
                            total_rows=total_rows, total_bytes=total_bytes, total_gb=gb,
                            total_duration=total_duration))

    return events


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

ingest_data = PythonOperator(
    dag=dag,
    task_id='ingest_data',
    python_callable=ingest_source_files,
    provide_context=True
)

announce_ingest = PythonOperator(
    dag=dag,
    task_id='announce_completion',
    python_callable=send_pubsub_message,
    provide_context=True
)

move_files = PythonOperator(
    dag=dag,
    task_id='move_files',
    python_callable=move_source_files,
    provide_context=True
)

list_files >> copy_files >> ingest_data >> announce_ingest >> move_files
