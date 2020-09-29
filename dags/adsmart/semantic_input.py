from datetime import timedelta, datetime
from pathlib import Path
from airflow import DAG
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import AirflowException
from common.sky_pubsub_sensor import SkyPubSubSensor
from google.cloud import bigquery
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
TABLE_NAMES = PARAMS['table_names']
PUBSUB_SUBSCRIPTION = PARAMS['pubsub_subscription']
NOTIFICATION_PROJECT = PARAMS['notification_project']
NOTIFICATION_TOPIC = PARAMS['notification_topic']
TARGET_PROJECT = PARAMS['target_project']
TARGET_DATASET = PARAMS['target_dataset']
SOURCE_PROJECT = PARAMS['source_project']
SOURCE_DATASET = PARAMS['source_dataset']
BQ_CONN_ID = PARAMS.get('bq_conn_id', 'google_cloud_default')
SOURCE_BQ_CONN_ID = PARAMS.get('source_bq_conn_id')
SOURCE_GCS_CONN_ID = PARAMS.get('source_conn_id', 'google_cloud_default')
KEY_PATH = PARAMS['key_path']
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


def copy_data_to_repo(table, **context):
    updated_tables = context['ti'].xcom_pull(dag_id=context['dag'].dag_id.split('.')[0], task_ids='pubsub_sensor')
    if len(updated_tables) == 0:
        raise AirflowException('Failing DAG as no files to process!')

    client = bigquery.Client.from_service_account_json(project=TARGET_PROJECT, json_credentials_path=KEY_PATH)
    events = []

    col_name = TABLE_NAMES[table]

    for updated_table, _ in sorted(updated_tables.items(), key=lambda x: (x, x[1]['created'])):
        if table[:-1] == '_'.join(updated_table.split('_')[:-1]).lower():
            source_table = '.'.join((SOURCE_PROJECT, SOURCE_DATASET, updated_table))
            destination_table = '.'.join((TARGET_PROJECT, TARGET_DATASET, table))
            if table == 'campaign_summary_aggregated':
                start_time = datetime.now().replace(microsecond=0)
                job_config = bigquery.CopyJobConfig()
                job_config.write_disposition = 'WRITE_TRUNCATE'
                job_config.create_disposition = 'CREATE_IF_NEEDED'
                copy_job = client.copy_table(source_table, destination_table, job_config=job_config)

                query = ''' SELECT count(*) c FROM `{sourcetable}`'''.format(sourcetable=source_table)

                query_job = client.query(query)  # API request - starts the query
                output = query_job.result()
                job_duration = (datetime.now().replace(microsecond=0) - start_time)
                total_rows = list(output)[0]['c']

                logging.info("Tag:{tag}|Project:{project}|Source:{source_table}|Destination:{destination_table}|"
                             "TotalRows:{total_rows}|JobType:{job_type}|TotalDuration:{total_duration}".
                             format(tag='ingest_data_from_source_table_to_destination_table',
                                    project=TARGET_PROJECT, source_table=source_table,
                                    destination_table=destination_table, total_rows=total_rows,
                                    job_type=copy_job.job_type, total_duration=job_duration))
                events.append(datetime.utcnow().isoformat())
            else:
                sql = '''SELECT {} FROM `{}` GROUP BY {}'''.format(col_name, source_table, col_name)
                results = client.query(sql)
                start_time = datetime.now().replace(microsecond=0)

                partition_days = [day.get(col_name).strftime('%Y-%m-%d') for day in results]

                with Pool(processes=10) as pool:
                    results = pool.map(partial(copy_source_table_partition_from_query, table, source_table,
                                               col_name,), partition_days)
                    total_rows = sum(results)
                total_duration = (datetime.now().replace(microsecond=0) - start_time)

                if total_rows == 0:
                    raise AirflowException("Failing DAG as no rows inserted for table {}".format(destination_table))

                # Log job summary
                logging.info("Tag:{tag}|Project:{project}|Source:{source_table}|Destination:{destination_table}|"
                             "TotalRows:{total_rows}|JobType:{job_type}|TotalDuration:{total_duration}".
                             format(tag='ingest_data_from_source_table_to_destination_table',
                                    project=TARGET_PROJECT, source_table=source_table,
                                    destination_table=destination_table, total_rows=total_rows, job_type="load",
                                    total_duration=total_duration))

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


def send_pubsub_message(table, **context):
    event_times = context['ti'].xcom_pull(task_ids='transfer_data_to_repo_{}'.format(table))
    if event_times:
        count = len(event_times)
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
            } for event_time in event_times
        ]
        PubSubHook(gcp_conn_id=PUBSUB_CONN_ID).publish(NOTIFICATION_PROJECT, NOTIFICATION_TOPIC, messages)
    else:
        logging.info("Table {} was not updated.".format(table))


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Implementation of L4 step 7.',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1
)

pub_sub_sensor = SkyPubSubSensor(
    project=PUBSUB_SUBSCRIPTION.split('.')[0],
    subscription=PUBSUB_SUBSCRIPTION,
    table_names=[t[:-1] for t in TABLE_NAMES.keys()],
    timestamp='%Y-%m-%d %H:%M:%S.%f',
    gcp_conn_id=SOURCE_GCS_CONN_ID,
    execution_timeout=timedelta(minutes=SENSOR_TIMEOUT),
    dag=dag,
    task_id='pubsub_sensor',
    poke_interval=60,
    timeout=(SENSOR_TIMEOUT - 5) * 60  # timeout 5 mins early so that there is a buffer between the instances
)

acknowledge_updated = PythonOperator(
    dag=dag,
    task_id='acknowledge_updated_tables',
    python_callable=acknowledge_updated_tables,
    op_kwargs={'subscription': PUBSUB_SUBSCRIPTION},
    provide_context=True
)

for table in sorted(TABLE_NAMES):
    transfer_data_to_repo = PythonOperator(
        dag=dag,
        task_id='transfer_data_to_repo_{}'.format(table),
        python_callable=copy_data_to_repo,
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

    pub_sub_sensor >> transfer_data_to_repo >> announce_ingest >> acknowledge_updated
