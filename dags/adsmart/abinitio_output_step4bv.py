from datetime import timedelta, datetime
from pathlib import Path
from airflow import DAG
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import AirflowException
from google.cloud import bigquery
from common.sky_pubsub_sensor import SkyPubSubSensor
from subprocess import check_output, call
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
SCHEDULE_INTERVAL = PARAMS.get('schedule_interval') or None

PUBSUB_CONN_ID = PARAMS.get('pubsub_conn_id', 'google_cloud_default')
TABLE_DICT = PARAMS['table_name']
TABLE_NAME = list(TABLE_DICT.keys())[0]
COL_NAME = TABLE_DICT[TABLE_NAME]
PUBSUB_SUBSCRIPTION = PARAMS['pubsub_subscription']
NOTIFICATION_PROJECT = PARAMS['notification_project']
NOTIFICATION_TOPIC = PARAMS['notification_topic']
TARGET_PROJECT = PARAMS['target_project']
TARGET_DATASET = PARAMS['target_dataset']
SOURCE_PROJECT = PARAMS['source_project']
SOURCE_DATASET = PARAMS['source_dataset']
BQ_CONN_ID = PARAMS.get('bq_conn_id', 'google_cloud_default')
BACKUP_PREFIX = PARAMS.get('backup_prefix')
SOURCE_BUCKET = PARAMS.get('source_bucket')
SOURCE_GCS_CONN_ID = PARAMS.get('source_conn_id', 'google_cloud_default')
KEY_PATH = PARAMS.get('key_path')
SENSOR_TIMEOUT = PARAMS['sensor_timeout']


def table_exists(client, table_ref):
    try:
        client.get_table(table_ref)
        return True
    except Exception:
        return False


def copy_source_table_partition_from_query(target_table, source_table, partition_col, partition_date):
    client = bigquery.Client.from_service_account_json(project=TARGET_PROJECT, json_credentials_path=KEY_PATH)
    # step 1 - delete the partition table
    start_time = datetime.now().replace(microsecond=0)
    dest_table_ref = client.dataset(TARGET_DATASET).table(target_table)

    destination_table = '.'.join((TARGET_PROJECT, TARGET_DATASET, target_table))
    destination_table = '{}${}'.format(destination_table, str(partition_date).replace('-', ''))
    if table_exists(client, dest_table_ref):
        client.delete_table(destination_table)

    # Select data by partition
    query = ''' SELECT * 
            FROM `{sourcetable}`
            WHERE {partitioncol}
            BETWEEN "{partitiondate}" AND "{partitiondate}"'''.format(sourcetable=source_table,
                                                                      partitioncol=partition_col,
                                                                      partitiondate=partition_date)
    job_config = bigquery.QueryJobConfig()
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.time_partitioning = bigquery.table.TimePartitioning(field=partition_col)
    job_config.destination = dest_table_ref

    query_job = client.query(
        query,
        job_config=job_config)  # API request - starts the query

    query_job.result()

    query = ''' SELECT count(*) c 
            FROM `{sourcetable}`
            WHERE {partitioncol}
            BETWEEN "{partitiondate}" AND "{partitiondate}"'''.format(sourcetable=source_table,
                                                                      partitioncol=partition_col,
                                                                      partitiondate=partition_date)

    query_job = client.query(query)  # API request - starts the query
    output = query_job.result()
    output_rows = list(output)[0]['c']

    job_duration = (datetime.now().replace(microsecond=0) - start_time)

    # Log job summary
    logging.info("Tag:{tag}|Project:{project}|Source:{source_table}|Destination:{destination_table}|"
                 "OutputRows:{output_rows}|JobType:{job_type}|Duration:{job_duration}".
                 format(tag='ingest_data_from_source_table_to_destination_table', source_table=source_table,
                        destination_table=destination_table, project=TARGET_PROJECT, output_rows=output_rows,
                        job_type="load", job_duration=job_duration))

    return output_rows


def copy_data_to_repo(**context):
    updated_tables = context['ti'].xcom_pull()

    if len(updated_tables) == 0:
        raise AirflowException('Failing DAG as no files to process!')

    client = bigquery.Client.from_service_account_json(project=TARGET_PROJECT, json_credentials_path=KEY_PATH)
    events = []
    table_name = TABLE_NAME
    col_name = COL_NAME

    for updated_table, _ in sorted(updated_tables.items(), key=lambda x: x[1]['created']):
        if table_name in updated_table:
            source_table = '.'.join((SOURCE_PROJECT, SOURCE_DATASET, updated_table))
            destination_table = '.'.join((TARGET_PROJECT, TARGET_DATASET, table_name))
            sql = '''SELECT {} FROM `{}` GROUP BY {}'''.format(col_name, source_table, col_name)
            results = client.query(sql)
            start_time = datetime.now().replace(microsecond=0)

            partition_days = [day.get(col_name).strftime('%Y-%m-%d') for day in results]

            with Pool(processes=10) as pool:
                results = pool.map(partial(copy_source_table_partition_from_query, table_name, source_table,
                                           col_name, ), partition_days)
                total_rows = sum(results)
            total_duration = (datetime.now().replace(microsecond=0) - start_time)

            if total_rows == 0:
                raise AirflowException("Failing DAG as no rows inserted")

            # Log job summary
            logging.info("Tag:{tag}|Project:{project}|Source:{source_table}|Destination:{destination_table}|"
                         "TotalRows:{total_rows}|JobType:{job_type}|TotalDuration:{total_duration}".
                         format(tag='ingest_data_from_source_table_to_destination_table',
                                project=TARGET_PROJECT, source_table=source_table, destination_table=destination_table,
                                total_rows=total_rows, job_type="load", total_duration=total_duration))

            events.append(datetime.utcnow().isoformat())

    return events


def acknowledge_updated_tables(subscription, **context):
    updated_tables = context['ti'].xcom_pull(dag_id=context['dag'].dag_id.split('.')[0], task_ids='pubsub_sensor')
    pubsub_hook = PubSubHook(gcp_conn_id=PUBSUB_CONN_ID)
    project, subscription = subscription.split('.')

    pubsub_hook.pull(project, subscription, 20, return_immediately=True)

    if updated_tables:
        for table, details in updated_tables.items():
            ack_ids = details['ack_ids']
            if ack_ids:
                logging.info("Acknowledging {} messages for {}".format(len(ack_ids), table))
                pubsub_hook.acknowledge(project, subscription, list(ack_ids))
    else:
        logging.info('Nothing to acknowledge!')


def send_pubsub_message(**context):
    event_times = context['ti'].xcom_pull(task_ids='transfer_data_to_repo')
    table_name = TABLE_NAME
    if event_times:
        count = len(event_times)
        logging.info(
            "Announcing {} change{} to table {}.".format(count, 's' if count > 1 else '', table_name)
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
                    "tableId": table_name,
                    "technology": "GCP.BIGQUERY.TABLE"
                }
            } for event_time in event_times
        ]
        PubSubHook(gcp_conn_id=PUBSUB_CONN_ID).publish(NOTIFICATION_PROJECT, NOTIFICATION_TOPIC, messages)
    else:
        logging.info("Table {} was not updated.".format(table_name))


def backup_files(**context):
    updated_tables = context['ti'].xcom_pull(dag_id=context['dag'].dag_id.split('.')[0], task_ids='pubsub_sensor')

    if BACKUP_PREFIX is not None and updated_tables:
        for _, details in updated_tables.items():
            ok_file_source = details['ok_file']
            ok_file_date = ok_file_source.split("_")[-1][:-3]
            source_base_folder = ok_file_source.split('/OK/')[0]

            ls_command = '/snap/bin/gsutil ls gs://{bucket_name}/{source_base_folder}' \
                         '/*/non_json_viewing_event_merge_{ok_file_date}.dat'.\
                format(bucket_name=SOURCE_BUCKET, source_base_folder=source_base_folder, ok_file_date=ok_file_date)

            # Ignore if there are no files
            try:
                output = check_output(ls_command, shell=True)
            except Exception as e:
                logging.info('No files found to move so skipping!!')
                logging.info(str(e))
                continue

            if output:
                for source_url in output.decode('ascii').split('\n'):
                    if source_url:
                        destination_url = source_url.replace(SOURCE_BUCKET + '/', SOURCE_BUCKET + '/' + BACKUP_PREFIX)
                        logging.info("Moving Source file from {} to {}".format(source_url, destination_url))
                        command = '/snap/bin/gsutil mv {source_url} {destination_url}'.\
                            format(source_url=source_url, destination_url=destination_url)
                        call(command, shell=True)

            ok_file_destination = BACKUP_PREFIX + ok_file_source
            logging.info("Moving OK file from gs://{}/{} to gs://{}/{}".format(
                SOURCE_BUCKET, ok_file_source, SOURCE_BUCKET, ok_file_destination
            ))
            command = '/snap/bin/gsutil mv gs://{bucket}/{source_object} gs://{bucket}/{destination_object}'. \
                format(bucket=SOURCE_BUCKET, source_object=ok_file_source, destination_object=ok_file_destination)

            call(command, shell=True)


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Implementation of L4 step 4bv.',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1
)

pub_sub_sensor = SkyPubSubSensor(
    project=SOURCE_PROJECT,
    subscription=PUBSUB_SUBSCRIPTION,
    table_names=list(TABLE_DICT.keys()),
    timestamp='%Y-%m-%dT%H:%M:%S.%fZ',
    gcp_conn_id=SOURCE_GCS_CONN_ID,
    execution_timeout=timedelta(minutes=SENSOR_TIMEOUT),
    dag=dag,
    task_id='pubsub_sensor',
    poke_interval=120,
    timeout=(SENSOR_TIMEOUT - 5) * 60  # timeout 5 mins early so that there is a buffer between the instances
)

transfer_data_to_repo = PythonOperator(
    dag=dag,
    task_id='transfer_data_to_repo',
    python_callable=copy_data_to_repo,
    provide_context=True
)

announce_completion = PythonOperator(
    dag=dag,
    task_id='announce_completion',
    python_callable=send_pubsub_message,
    provide_context=True
)

acknowledge_updated = PythonOperator(
    dag=dag,
    task_id='acknowledge_updated',
    python_callable=acknowledge_updated_tables,
    op_kwargs={'subscription': PUBSUB_SUBSCRIPTION},
    provide_context=True
)

files_backup = PythonOperator(
    dag=dag,
    task_id='files_backup',
    python_callable=backup_files,
    provide_context=True
)

pub_sub_sensor >> transfer_data_to_repo >> announce_completion >> acknowledge_updated >> files_backup
