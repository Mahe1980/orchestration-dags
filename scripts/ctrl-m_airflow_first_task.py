from datetime import datetime
import requests
import json
import time
import argparse
import logging
import re
import sys
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

SERVER = {
    'dev': {
        'host': '35.233.17.115',
        'port': '8082',
        'project_id': 'skyuk-uk-ds-orcs-dev',
        'logs_bucket': 'orcs-airflow-20180621'
    },
    'prod': {
        'host': '35.195.61.30',
        'port': '8082',
        'project_id': 'skyuk-uk-ds-orcs-prod',
        'logs_bucket': 'orcs-airflow-1526030608'
    }
}


def get_logs_from_stackdriver(env, dag_id, task_id, exec_date):
    logging.info('Collecting logs from Stackdriver. Please wait..')
    time.sleep(60)
    project_id = SERVER[env]['project_id']
    logname = 'projects/{}/logs/airflow'.format(project_id)
    exec_date_filter = re.sub('-|:|%3A', '', exec_date)
    exec_date_filter = re.sub('%2B', '+', exec_date_filter)

    filter_str = 'logName={} AND jsonPayload.tailed_path:{}/{}/{}'. \
        format(logname, dag_id, task_id, exec_date_filter)

    url = 'https://logging.googleapis.com/v2/entries:list'
    data = {
        "projectIds": [
            project_id
        ],
        "pageSize": 1000,
        "filter": filter_str
    }

    SCOPE = ["https://www.googleapis.com/auth/cloud-platform",
             "https://www.googleapis.com/auth/cloud-platform.read-only",
             "https://www.googleapis.com/auth/logging.admin",
             "https://www.googleapis.com/auth/logging.read"]
    token = ServiceAccountCredentials.from_json_keyfile_name('orcs-dev.json', SCOPE).get_access_token().access_token

    response = requests.post(url, data=data, headers={'Authorization': 'Bearer {}'.format(token)})
    response_json = json.loads(response.text)

    if 'entries' in response_json.keys():
        for entry in response_json['entries']:
            print(entry['jsonPayload']['message'])

    while True:
        if 'nextPageToken' in response_json.keys():
            data['pageToken'] =  response_json['nextPageToken']
            response = requests.post(url, data=data, headers={'Authorization': 'Bearer {}'.format(token)})
            response_json = json.loads(response.text)

            if 'entries' in response_json.keys():
                for entry in response_json['entries']:
                    print(entry['jsonPayload']['message'])
        elif 'error' in response_json.keys():
            print(response_json['error']['message'])
            break
        else:
            break


def run(dag_id, task_id, run_id, execution_date, env, poll_interval):
    host = SERVER[env]['host']
    port = SERVER[env]['port']

    # Trigger Dag
    base_url = 'http://{host}:{port}/admin/rest_api/api?api=trigger_dag&dag_id={dag_id}&run_id={run_id}&' \
               'exec_date={exec_date}'.format(host=host, port=port, dag_id=dag_id, run_id=run_id, exec_date=exec_date)
    logging.info('Triggering dag "{}"'.format(dag_id))
    response = requests.post(base_url)

    d = json.loads(response.text)

    call_status = d['status']
    if call_status == 'ERROR':
        logging.error('Error in Triggering the dag "{}"'.format(dag_id))
        logging.error(d['output'])
        sys.exit(-1)

    stderr = d['output']['stderr']
    if stderr != '':
        logging.error('Error in Triggering the dag "{}"'.format(dag_id))
        for line in stderr.split('\\n'):
            print(line)
        sys.exit(-1)

    # Check State of the dag
    state_url = 'http://{host}:{port}/admin/rest_api/api?api=dag_state&dag_id={dag_id}&' \
                'execution_date={exec_date}'.format(host=host, port=port, dag_id=dag_id, exec_date=exec_date)

    logging.info('Checking state of the dag "{}"'.format(dag_id))

    while True:
        response = requests.post(state_url)
        d = json.loads(response.text)

        call_status = d['status']
        if call_status == 'ERROR':
            logging.error('Error in getting the state of the dag "{}"'.format(dag_id))
            logging.error(d['output'])
            sys.exit(-1)

        stderr = d['output']['stderr']
        if stderr != '':
            logging.error('Error in Triggering the dag "{}"'.format(dag_id))
            for line in stderr.split('\\n'):
                print(line)
            sys.exit(-1)

        status = d['output']['stdout'].split('\\n')[-2]
        if 'failed' in status:
            logging.error('Failed')
            # get_logs_from_stackdriver(env, dag_id, exec_date)
            break
        elif 'running' in status:
            logging.info('Running')
            time.sleep(poll_interval)
        else:
            logging.info(status[3:])
            time.sleep(poll_interval)


    execution_date = execution_date.replace(':', '%3A')
    execution_date = execution_date.replace('+', '%2B')

    state_url = "http://{host}:{port}/admin/rest_api/api?api=task_state&dag_id={dag_id}" \
                "&execution_date={execution_date}".format(host=host, port=port, dag_id=dag_id,
                                                          execution_date=execution_date)
    run_url = 'http://{host}:{port}/admin/rest_api/api?api=run&dag_id={dag_id}&task_id={task_id}' \
              '&execution_date={execution_date}'.format(host=host, port=port, dag_id=dag_id,
                                                        task_id=task_id, execution_date=execution_date)
    logging.info('Running task_id {task_id}'.format(task_id=task_id))
    response = requests.post(run_url)

    d = json.loads(response.text)
    call_status = d['status']
    if call_status == 'ERROR':
        logging.error('Error in running the task_id "{}"'.format(task_id))
        logging.error(d['output'])
        sys.exit(-1)

    stderr = d['output']['stderr']
    if stderr != '':
        logging.error('Error in running the task_id "{}"'.format(task_id))
        for line in stderr.split('\\n'):
            print(line)
        # sys.exit(-1)


    task_state_url = "{}&task_id={task_id}".format(state_url, task_id=task_id)

    while True:
        response = requests.post(task_state_url)
        d = json.loads(response.text)

        call_status = d['status']
        if call_status == 'ERROR':
            logging.error('Error in getting status of the task_id "{}"'.format(task_id))
            logging.error(d['output'])
            sys.exit(-1)

        stderr = d['output']['stderr']
        if stderr != '':
            logging.error('Error in getting status of the task_id "{}"'.format(task_id))
            for line in stderr.split('\\n'):
                print(line)
            # sys.exit(-1)

        status = d['output']['stdout'].split('\\n')[-2]
        if 'failed' in status:
            logging.info('Failed')
            #get_logs_from_stackdriver(env, dag_id, task_id, execution_date)
            sys.exit(-1)
        elif 'success' in status:
            logging.info('Success')
            #get_logs_from_stackdriver(env, dag_id, task_id, execution_date)
            sys.exit(0)
        elif 'running' in status:
            logging.info('Running')
            time.sleep(poll_interval)
        else:
            logging.info(status[3:])
            time.sleep(poll_interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dag_id', help='DAG ID')
    parser.add_argument('task_id', help='Task ID')

    exec_date = datetime.utcnow()
    exec_date = exec_date.strftime('%Y-%m-%dT%H:%M:%S')
    run_id = 'ctrlm_{}'.format(exec_date)
    parser.add_argument('--run_id', default=run_id, help='RUN ID')
    parser.add_argument('--execution_date', default=exec_date, help='Execution Date')

    # parser.add_argument('execution_date', help='Execution Date. E.g. 2018-01-01-T03:04:05')
    parser.add_argument('--env', default='dev', choices=SERVER.keys(), help='DAG Environment')
    parser.add_argument('--poll_interval', default=30, type=int, help='Polling Interval for the Status')
    args = parser.parse_args()
    run(**vars(args))
