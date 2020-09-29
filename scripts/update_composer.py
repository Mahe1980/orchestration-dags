import argparse
from subprocess import call


def run(composer, project, location):

    command = "gcloud beta composer environments update {} --project={} --location {} --update-pypi-packages-from-file " \
              "requirements.txt".format(composer, project, location)

    print(command)
    call(command, shell=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('composer', help='Name of the Composer')
    parser.add_argument('project', help='Name of the Project')
    parser.add_argument('location', help='Location')

    args = parser.parse_args()
    run(**vars(args))
