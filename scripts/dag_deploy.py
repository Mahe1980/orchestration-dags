#!/usr/bin/env python3
import argparse
from modulefinder import ModuleFinder
from pathlib import Path
from subprocess import call

ENVIRONMENT = {
    'dev': {
        'project': 'skyuk-uk-ds-orcs-dev',
        'server': 'airflow-master-dev',
    },
    'test': {
        'project': 'skyuk-uk-ds-orcs-test',
        'server': 'airflow-test',
    },
    'prod': {
        'project': 'skyuk-uk-ds-orcs-prod',
        'server': 'airflow-prod',
    }
}

DAGS_PATH = Path(__file__).absolute().parent.parent.parent / 'orchestration-dags/dags/'
DEFAULT_AIRFLOW_HOME = '/opt/airflow/dags/'


def copy_files(file_path, finder, project, server, user, airflow_home):

    finder.run_script(file_path)

    for module in finder.modules.values():
        if module.__file__:
            source_path = Path(module.__file__)
            destination_path = Path(airflow_home, source_path.relative_to(DAGS_PATH))

            command = f'gcloud compute --project={project} scp {source_path} {user}@{server}:{destination_path}'
            call(command, shell=True)
            # TODO: explore if it makes sense to use google api client instead of calling `gcloud`


def run(env, path, airflow_home):
    project = ENVIRONMENT[env]['project']
    server = ENVIRONMENT[env]['server']
    user = ENVIRONMENT[env].get('user', 'airflow')

    dags_abs_path = path

    if not Path(path).is_absolute():
        dags_abs_path = DAGS_PATH / path

    finder = ModuleFinder(path=[DAGS_PATH])

    if Path(dags_abs_path).is_dir():
        p = Path(dags_abs_path).glob('*.py')
        files = [x for x in p if x.is_file()]
        for f in files:
            copy_files(f, finder, project, server, user, airflow_home)
    else:
        copy_files(dags_abs_path, finder, project, server, user, airflow_home)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='DAG file path.')
    parser.add_argument('--env', default='dev', choices=ENVIRONMENT.keys(), help='deployment environment')
    parser.add_argument('--airflow-home', default=DEFAULT_AIRFLOW_HOME, help='Remote base directory.')
    args = parser.parse_args()
    run(**vars(args))
