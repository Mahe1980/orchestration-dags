import argparse
from subprocess import call


def run(composer, project, location, zone, service_account, node_count, disk_size, oauth_scopes, python_version,
        airflow_version):

    command = "gcloud beta composer environments create {} --project={} --location {} --zone={} --service-account={}" \
              " --node-count={} --disk-size={} --oauth-scopes={} --python-version={} --airflow-version={}"\
        .format(composer, project, location, zone, service_account, node_count, disk_size, oauth_scopes,
                python_version, airflow_version)

    print(command)
    call(command, shell=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('composer', help='Name of the Composer')
    parser.add_argument('project', help='Name of the Project')
    parser.add_argument('location', help='Location')
    parser.add_argument('zone', help='Zone')
    parser.add_argument('--service_account', default='', help='Service Account')
    parser.add_argument('--node_count', default=3, type=int, help='Node Count')
    parser.add_argument('--disk_size', default="100GB", help='Disk Size')
    parser.add_argument('--oauth_scopes', default="", help="Auth scopes")
    parser.add_argument('--python_version', default=3, help="Python Version")
    parser.add_argument('--airflow_version', default="1.10.0", help="Airflow Version")

    args = parser.parse_args()
    run(**vars(args))
