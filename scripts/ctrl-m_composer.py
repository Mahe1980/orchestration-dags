from datetime import datetime
import requests
import json
import time
import argparse
import logging
import re
import sys
from google.cloud.logging import Client
from google.cloud.logging import ASCENDING
from oauth2client.service_account import ServiceAccountCredentials


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

ENVIRONMENT = {
    'dev': {
        'host': 'kdd2d61caded150bc-tp.appspot.com',
        'project_id': 'skyuk-uk-ds-orcs-dev',
        'credentials': 'orcs-dev.json'
    },
    'prod': {
        'host': 'update_later',
        'project_id': 'skyuk-uk-ds-orcs-prod',
        'credentials': 'orcs-dev.json'
    }
}


def get_logs_from_stackdriver(env, dag_id, exec_date):
    logging.info('Collecting logs from Stackdriver. Please wait..')
    time.sleep(30)
    # Instantiate a client
    logging_client = Client.from_service_account_json('orcs-{}.json'.format(env))
    project_id = ENVIRONMENT[env]['project_id']
    logname = 'projects/{}/logs/airflow'.format(project_id)
    exec_date_filter = re.sub('-|:', '', exec_date)

    # TODO: Add timestamp and resource type to the filter
    filter_str = 'logName={} AND jsonPayload.tailed_path:{}+0000 AND jsonPayload.tailed_path:{}'.\
        format(logname, exec_date_filter, dag_id)

    iterator = logging_client.list_entries(projects=[ENVIRONMENT[env]['project_id']], order_by=ASCENDING,
                                           filter_=filter_str, page_size=1000)
    for page in iterator.pages:
        time.sleep(3)
        for entry in page:
            print(entry.payload['message'])


def run(dag_id, env, poll_interval, run_id):
    host = ENVIRONMENT[env]['host']
    credentials = ENVIRONMENT[env]['credentials']
    exec_date= run_id.split('_')[1]

    SCOPE = ["https://www.googleapis.com/auth/cloud-platform"]
    token = ServiceAccountCredentials.from_json_keyfile_name(credentials, SCOPE).get_access_token().access_token

    # Trigger Dag
    base_url = 'https://{host}/admin/rest_api/api?api=trigger_dag&dag_id={dag_id}&run_id={run_id}&' \
               'exec_date={exec_date}'.format(host=host, dag_id=dag_id, run_id=run_id, exec_date=exec_date)
    logging.info('Triggering dag "{}"'.format(dag_id))

    response = requests.post(base_url, headers={'Authorization': 'Bearer %s' % token})
    d = json.loads(response.text)
    call_status = d['status']

    if call_status == 'ERROR':
        logging.error('Error in Triggering the dag "{}"'.format(dag_id))
        logging.error(d['output'])
        sys.exit(-1)

    # Check State of the dag
    state_url = 'https://{host}/admin/rest_api/api?api=dag_state&dag_id={dag_id}&' \
                'execution_date={exec_date}'.format(host=host, dag_id=dag_id, exec_date=exec_date)

    logging.info('Checking state of the dag "{}"'.format(dag_id))

    while True:
        response = requests.post(state_url)
        d = json.loads(response.text)

        call_status = d['status']
        if call_status == 'ERROR':
            logging.error('Error in getting the state of the dag "{}"'.format(dag_id))
            logging.error(d['output'])
            sys.exit(-1)

        status = d['output']['stdout'].split('\\n')[-2]
        if 'success' in status:
            logging.info('Success')
            get_logs_from_stackdriver(env, dag_id, exec_date)
            sys.exit(0)
        elif 'failed' in status:
            logging.error('Failed')
            get_logs_from_stackdriver(env, dag_id, exec_date)
            sys.exit(-1)
        elif 'running' in status:
            logging.info('Running')
            time.sleep(poll_interval)
        else:
            logging.info(status[3:])
            time.sleep(poll_interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dag_id', help='DAG ID')
    exec_date = datetime.utcnow()
    exec_date = exec_date.strftime('%Y-%m-%dT%H:%M:%S')
    run_id = 'ctrl-m_{}'.format(exec_date)
    parser.add_argument('--env', default='dev', choices=ENVIRONMENT.keys(), help='DAG Environment')
    parser.add_argument('--poll_interval', default=30, type=int, help='Polling Interval for the Status')
    parser.add_argument('--run_id', default=run_id, help='RUN ID')
    args = parser.parse_args()
    run(**vars(args))
