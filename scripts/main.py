from google.cloud import storage
from datetime import datetime
import time


def my_dpm_fun(data, context):
    time.sleep(60)
    time_now = datetime.now().strftime("%Y-%m-%d:%H:%M:%S")
    filename = 'tmp_' + time_now + '.txt'
    client = storage.Client.from_service_account_json('orcs-dev.json')
    bucket = client.get_bucket('test-dpm-file-copy')
    blob = bucket.blob(filename)
    blob.upload_from_string('dpm dummy file')
