from datetime import datetime, timedelta
import requests
import json
import time
import argparse
import logging
import re
import sys
import subprocess
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
SLEEP_TIME = 60


SERVER = {
    'orcs-dev': {
        'host': '35.233.17.115',
        'port': '8082',
        'project_id': 'skyuk-uk-ds-orcs-dev',
        'logs_bucket': 'orcs-airflow-20180621'
    },
    'orcs-prod': {
        'host': '35.195.61.30',
        'port': '8082',
        'project_id': 'skyuk-uk-ds-orcs-prod',
        'logs_bucket': 'orcs-airflow-1526030608'
    },
    'viewing-proc-prod': {
        'host': '104.155.47.46',
        'port': '8080',
        'project_id': 'skyuk-uk-viewing-proc-prod'
    },
    'viewing-proc-dev': {
        'host': '35.241.227.61',
        'port': '8080',
        'project_id': 'skyuk-uk-viewing-proc-dev'
    },
    'semantic-prod': {
        'host': '35.195.178.155',
        'port': '8080',
        'project_id': 'skyuk-uk-ids-semantic-prod',
    },
    'semantic-dev': {
        'host': '35.195.193.3',
        'port': '8080',
        'project_id': 'sky-uk-ids-semantic-dev',
    }
}


def get_logs_from_stackdriver(env, dag_id, exec_date_filter, log_severity):
    logging.info('Collecting logs from Stackdriver. Please wait {} seconds'.format(SLEEP_TIME))
    time.sleep(SLEEP_TIME)
    project_id = SERVER[env]['project_id']
    logname = 'projects/{}/logs/airflow'.format(project_id)

    ts = datetime.strptime(exec_date_filter, '%Y-%m-%dT%H:%M:%S') - timedelta(minutes=2)
    ts = ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    if env in ('semantic-dev', 'semantic-prod', 'viewing-proc-prod', 'viewing-proc-dev'):
        filter_str = 'timestamp > \"{ts}\" AND severity >= {log_severity} AND resource.type=gce_instance AND ' \
                     'resource.labels.project_id={project_id} AND logName={logname} AND ' \
                     'jsonPayload.tailed_path:{dag_id} AND jsonPayload.tailed_path:"{exec_date_filter}"'.\
            format(ts=ts, log_severity=log_severity, project_id=project_id, logname=logname, dag_id=dag_id,
                   exec_date_filter=exec_date_filter)
    else:
        exec_date_filter = re.sub('[-:]', '', exec_date_filter)
        filter_str = 'timestamp > \"{ts}\" AND severity >= {log_severity} AND resource.type=gce_instance ' \
                     'AND resource.labels.project_id={project_id} AND logName={logname} AND ' \
                     'jsonPayload.tailed_path:{dag_id} AND jsonPayload.tailed_path:{exec_date_filter}+0000'.\
            format(ts=ts, log_severity=log_severity, project_id=project_id, logname=logname, dag_id=dag_id,
                   exec_date_filter=exec_date_filter)

    url = 'https://logging.googleapis.com/v2/entries:list'
    data = {
        "projectIds": [
            project_id
        ],
        "pageSize": 1000,
        "filter": filter_str
    }

    scope = ["https://www.googleapis.com/auth/cloud-platform",
             "https://www.googleapis.com/auth/cloud-platform.read-only",
             "https://www.googleapis.com/auth/logging.admin",
             "https://www.googleapis.com/auth/logging.read"]
    token = ServiceAccountCredentials.from_json_keyfile_name(env + '.json', scope).get_access_token().access_token

    response = requests.post(url, data=data, headers={'Authorization': 'Bearer {}'.format(token)})
    response_json = json.loads(response.text)

    if 'entries' in response_json.keys():
        for entry in response_json['entries']:
            print(entry['jsonPayload']['message'])

    while True:
        if 'nextPageToken' in response_json.keys():
            data['pageToken'] = response_json['nextPageToken']
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


def get_logs_from_gcs(env, dag_id, exec_date):
    logging.info('Collecting logs from GCS. Please wait..')
    time.sleep(30)
    exec_date_filter = re.sub('[-:]', '', exec_date)
    exec_date_filter = '{}+0000'.format(exec_date_filter)
    ls_command = 'gsutil ls -l gs://{}/logs/{}/*/{}/*.*'.format(SERVER[env]['logs_bucket'], dag_id, exec_date_filter)
    try:
        ret_val = subprocess.check_output(ls_command, shell=True)
        if ret_val:
            ts_filename = dict()
            for line in ret_val.decode('ascii').split('\n'):
                try:
                    _, ts, file_path = line.split()
                    ts_filename[file_path] = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ')
                except:
                    pass
            for file_name in sorted(ts_filename, key=ts_filename.__getitem__):
                task_name = file_name.split('/')[-3]
                print('Task Name: {}'.format(task_name))
                cat_command = 'gsutil cat {}'.format(file_name)
                cat_output = subprocess.check_output(cat_command, shell=True)
                print('*' * 75)
                print(cat_output.decode('ascii'), end='')
                print('*' * 75)
    except Exception as e:
        print(str(e))


def run(dag_id, env, poll_interval, run_id, log_location, log_severity):
    host = SERVER[env]['host']
    port = SERVER[env]['port']
    exec_date = run_id.split('_')[1]

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
        if 'success' in status:
            logging.info('Success')
            if log_location == 'stackdriver':
                get_logs_from_stackdriver(env, dag_id, exec_date, log_severity)
            else:
                get_logs_from_gcs(env, dag_id, exec_date)
            sys.exit(0)
        elif 'failed' in status:
            logging.error('Failed')
            if log_location == 'stackdriver':
                get_logs_from_stackdriver(env, dag_id, exec_date, log_severity)
            else:
                get_logs_from_gcs(env, dag_id, exec_date)
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
    timestamp = datetime.utcnow()
    timestamp = timestamp.strftime('%Y-%m-%dT%H:%M:%S')
    runid = 'ctrl-m_{}'.format(timestamp)
    parser.add_argument('--env', default='dev', choices=SERVER.keys(), help='DAG Environment')
    parser.add_argument('--poll_interval', default=30, type=int, help='Polling Interval for the Status')
    parser.add_argument('--run_id', default=runid, help='RUN ID')
    parser.add_argument('--log_location', default='stackdriver', choices=('stackdriver', 'gcs'), help='log location')
    parser.add_argument('--log_severity', default='DEFAULT', choices=('DEFAULT', 'DEBUG', 'INFO', 'ERROR', 'WARNING'),
                        help='log level')
    args = parser.parse_args()
    run(**vars(args))
