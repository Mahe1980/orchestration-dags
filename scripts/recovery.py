from datetime import datetime
import requests
import json
import time
import argparse
import logging
import re
import sys
from oauth2client.service_account import ServiceAccountCredentials
from collections import OrderedDict

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


def run(dag_id, env, execution_date, poll_interval):
    host = SERVER[env]['host']
    port = SERVER[env]['port']

    list_tasks_url = 'http://{host}:{port}/admin/rest_api/api?api=list_tasks&dag_id={dag_id}&tree'.format(host=host,
                                                                                                          port=port,
                                                                                                          dag_id=dag_id)
    logging.info('Listing tasks for "{}"'.format(dag_id))
    response = requests.post(list_tasks_url)

    d = json.loads(response.text)

    call_status = d['status']
    if call_status == 'ERROR':
        logging.error('Error in Listing tasks for "{}"'.format(dag_id))
        logging.error(d['output'])
        sys.exit(-1)

    stderr = d['output']['stderr']
    if stderr != '':
        logging.error('Error in Listing tasks for "{}"'.format(dag_id))
        for line in stderr.split('\\n'):
            print(line)
        sys.exit(-1)

    stdout = d['output']['stdout']

    tasks = stdout.split("Filling up the DagBag from /opt/airflow/dags\\n'b'")[1]
    tasks = re.sub("<Task\(PythonOperator\): |>|\'b\'|\'", '', tasks)
    dag_tasks = []
    for task in tasks.split('\\n')[::-1]:
        if task:
            dag_tasks.append(task.strip())

    if dag_tasks:
        dag_tasks = OrderedDict.fromkeys(dag_tasks)

    print(dag_tasks.keys())

    execution_date = execution_date.replace(':', '%3A')
    execution_date = execution_date.replace('+', '%2B')
    state_url = "http://{host}:{port}/admin/rest_api/api?api=task_state&dag_id={dag_id}" \
                "&execution_date={execution_date}".format(host=host, port=port, dag_id=dag_id,
                                                          execution_date=execution_date)

    for task_id in dag_tasks:
        task_state_url = "{}&task_id={task_id}".format(state_url, task_id=task_id)
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
            sys.exit(-1)
        status = d['output']['stdout'].split('\\n')[-2]

        if 'success' in status:
            continue

        if 'failed' in status:
            run_url = 'http://{host}:{port}/admin/rest_api/api?api=run&dag_id={dag_id}&task_id={task_id}' \
                           '&execution_date={execution_date}'.format(host=host, port=port, dag_id=dag_id,
                                                                     task_id=task_id, execution_date=execution_date)
            logging.info('Running failed task_id {task_id}'.format(task_id=task_id))
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
                sys.exit(-1)

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
                    sys.exit(-1)

                status = d['output']['stdout'].split('\\n')[-2]
                if 'failed' in status:
                    logging.info('Failed')
                    get_logs_from_stackdriver(env, dag_id, task_id, execution_date)
                    sys.exit(-1)
                elif 'success' in status:
                    logging.info('Success')
                    get_logs_from_stackdriver(env, dag_id, task_id, execution_date)
                    break
                elif 'running' in status:
                    logging.info('Running')
                    time.sleep(poll_interval)
                else:
                    logging.info(status[3:])
                    time.sleep(poll_interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dag_id', help='DAG ID')
    parser.add_argument('execution_date', help='Execution Date. E.g. 2018-01-01-T03:04:05')
    parser.add_argument('--env', default='dev', choices=SERVER.keys(), help='DAG Environment')
    parser.add_argument('--poll_interval', default=30, type=int, help='Polling Interval for the Status')
    args = parser.parse_args()
    run(**vars(args))
