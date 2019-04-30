from io import BytesIO
from zipfile import ZipFile
import requests
import sys
import traceback
from django.core import mail

GBIF_API_DATASET_URL = "https://api.gbif.org/v1/dataset/{}"

def get_dataset_list():
    try:
        response = requests.get(GBIF_API_DATASET_URL.format('search?limit=5000&publishingCountry=NO'))
        response.raise_for_status()
        json = response.json()
        return json['results']
    except requests.exceptions.RequestException as e:
        _email_error(e)
        return []

def get_dataset_endpoints(dataset_key):
    try:
        response = requests.get(GBIF_API_DATASET_URL.format(dataset_key))
        response.raise_for_status()
        json = response.json()
        return json['endpoints']
    except requests.exceptions.RequestException as e:
        _email_error(e)
        return []

def get_dwc_endpoint(endpoints):
    darwin_core_endpoints = [endpoint for endpoint in endpoints if endpoint['type'] == 'DWC_ARCHIVE']
    return next(iter(darwin_core_endpoints), False)

def get_cores_from_ipt(url):
    try:
        response = requests.get(url, stream=True)
        #response.raise_for_status()
        with open('/tmp/tmp.zip', 'wb') as fd:
            for chunk in response.iter_content(5000):
                fd.write(chunk)
        print('finished writing')
        #with ZipFile(BytesIO(response.content)) as zipfile:
        with ZipFile('/tmp/tmp.zip') as zipfile:
            file_names = zipfile.namelist()
            print('got namelist')
            file_objects_and_names = [(fn[:-4], zipfile.open(fn)) for fn in file_names if fn[-3:] == 'txt']
            print('got file objects and names')
            # Returns e.g. [('occurrence', fileobj), ('multimedia', fileobj)] for occurrence.txt and multimedia.txt files
            return file_objects_and_names
    except requests.exceptions.RequestException as e:
        _email_error(e)
        return []

def _email_error(e):
    exc_info = sys.exc_info()
    subject = "Error in populating the resolver. GET request code: %s." % (e.response.status_code)
    message = "URL: %s\n\n%s" % (e.response.url, '\n'.join(traceback.format_exception(*exc_info)))
    mail.mail_admins(subject, message, fail_silently=True)

