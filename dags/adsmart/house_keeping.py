from datetime import datetime, timedelta
from pathlib import Path
import shutil
import subprocess
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

DAG_ID = '{p.parent.name}_{p.stem}'.format(p=Path(__file__))
PARAMS = Variable.get(DAG_ID, deserialize_json=True)

SCHEDULE_INTERVAL = PARAMS.get('schedule_interval') or timedelta(weeks=1)
BUCKET = PARAMS['logs_bucket']
DAYS_TO_RETAIN = PARAMS.get('days_to_retain', 7)


def clear_dag_logs():
    last_week = datetime.now() - timedelta(DAYS_TO_RETAIN)
    p = Path('/opt/airflow/logs/')

    for path in list(p.glob('*/*/*')):
        log_date = str(path).split('/')[-1]
        log_date = log_date.split('T')[0]
        try:
            log_date = datetime.strptime(log_date, '%Y%m%d')
        except:
            log_date = datetime.strptime(log_date, '%Y-%m-%d')

        if log_date < last_week:
            gcs_path = str(path.absolute()).split('airflow')[1]
            gcs_full_path = 'gs://{}{}'.format(BUCKET, gcs_path)
            ls_command = '/snap/bin/gsutil ls {}'.format(gcs_full_path)

            ret_val = subprocess.call(ls_command, shell=True)
            if ret_val:
                cp_command = '/snap/bin/gsutil -m cp -r {}/ {}/'.format(path.absolute(), gcs_full_path)
                ret_val = subprocess.call(cp_command, shell=True)

            if not ret_val:
                shutil.rmtree(str(path.absolute()))


dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='house keeping logs.',
    schedule_interval=SCHEDULE_INTERVAL
)

clear_logs = PythonOperator(
    dag=dag,
    task_id='clear_logs',
    python_callable=clear_dag_logs
)

dag >> clear_logs
