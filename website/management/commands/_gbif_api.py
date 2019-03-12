from io import BytesIO
from zipfile import ZipFile
import requests
import sys
import traceback
from django.core import mail
from django.views.debug import ExceptionReporter

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

def get_first_darwin_core_url_from_list(endpoints):
    return next(endpoint for endpoint in endpoints if endpoint['type'] == 'DWC_ARCHIVE')

def get_cores_from_ipt(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        with ZipFile(BytesIO(response.content)) as zipfile:
            file_names = zipfile.namelist()

            # Returns e.g. [('occurrence', fileobj), ('multimedia', fileobj)] for occurrence.txt and multimedia.txt files
            return [(fn[:-3], zipfile.open(fn)) for fn in file_names if fn[-3:] == 'txt']
    except requests.exceptions.RequestException as e:
        _email_error(e)
        return []

def _email_error(e):
    exc_info = sys.exc_info()
    subject = "Error in populating the resolver. GET request code: %s." % (e.response.status_code)
    message = "URL: %s\n\n%s" % (e.response.url, '\n'.join(traceback.format_exception(*exc_info)))
    mail.mail_admins(subject, message, fail_silently=True)

