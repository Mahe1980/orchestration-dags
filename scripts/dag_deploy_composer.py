#!/usr/bin/env python3
import argparse
from modulefinder import ModuleFinder
from pathlib import Path
from subprocess import call

ENVIRONMENT = {
    'dev': {
        'project': 'skyuk-uk-ds-orcs-dev',
        'bucket': 'europe-west1-l4-adsmart-714f2d3e-bucket',
        'credentials': 'orcs-dev.json'
    },
    'test': {
        'project': 'skyuk-uk-ds-orcs-test',
        'bucket': 'update_later',
        'credentials': 'orcs-test.json'
    },
    'prod': {
        'project': 'skyuk-uk-ds-orcs-prod',
        'bucket': 'update_later',
        'credentials': 'orcs-prod.json'
    }
}

DAGS_PATH = Path(__file__).absolute().parent.parent.parent / 'orchestration-dags/dags/'


def copy_files(file_path, finder, gs_path):

    finder.run_script(file_path)

    for module in finder.modules.values():
        if module.__file__:
            source_path = Path(module.__file__)
            destination_path = '{}/{}'.format(gs_path, source_path.relative_to(DAGS_PATH))

            cp_command = 'gsutil cp {} {}'.format(source_path, destination_path)
            call(cp_command, shell=True)
            # TODO: explore if it makes sense to use google api client instead of calling `gcloud`


def run(env, path):

    dags_abs_path = path

    if not Path(path).is_absolute():
        dags_abs_path = DAGS_PATH / path

    if not Path(dags_abs_path).exists():
        raise Exception('Unable to find the DAG file specified in the path. Please specify absolute or relative to dags'
                        ' folder path.')

    project = ENVIRONMENT[env]['project']
    credentials = ENVIRONMENT[env]['credentials']
    bucket = ENVIRONMENT[env]['bucket']

    finder = ModuleFinder(path=[DAGS_PATH])

    auth_command = 'gcloud auth activate-service-account --key-file {}'.format(credentials)
    call(auth_command, shell=True)

    proj_command = "gcloud config set project {}".format(project)
    call(proj_command, shell=True)

    gs_path = "gs://{}/dags".format(bucket)

    if Path(dags_abs_path).is_dir():
        p = Path(dags_abs_path).glob('*.py')
        files = [x for x in p if x.is_file()]
        for f in files:
            copy_files(f, finder, gs_path)
    else:
        copy_files(dags_abs_path, finder, gs_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='DAG file path.')
    parser.add_argument('--env', default='dev', choices=ENVIRONMENT.keys(), help='deployment environment')
    args = parser.parse_args()
    run(**vars(args))
