import logging
import requests
import sys
import traceback

GBIF_API_DATASET_URL = "https://api.gbif.org/v1/dataset/{}"


def get_dataset_list():
    try:
        response = requests.get(GBIF_API_DATASET_URL.format('search?limit=5000&publishingCountry=NO'))
        response.raise_for_status()
        json = response.json()
        return json['results']
    except requests.exceptions.RequestException as e:
        _log_error(e)
        return []


def get_dataset_detailed_info(dataset_key):
    try:
        response = requests.get(GBIF_API_DATASET_URL.format(dataset_key))
        response.raise_for_status()
        json = response.json()
        return json
    except requests.exceptions.RequestException as e:
        _log_error(e)
        return []


def get_dwc_endpoint(endpoints):
    darwin_core_endpoints = [endpoint for endpoint in endpoints if endpoint['type'] == 'DWC_ARCHIVE']
    return next(iter(darwin_core_endpoints), False)


def get_dwca_and_store_as_tmp_zip(url):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open('/tmp/tmp.zip', 'wb') as fd:
            for chunk in response.iter_content(5000):
                fd.write(chunk)
        return True
    except (requests.exceptions.SSLError, requests.exceptions.HTTPError) as e:
        logging.warning(f'SSL or HTTP error {e}')
        return False


def _log_error(e):
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    exc_info = sys.exc_info()
    msg = "GET request code: %s. URL: %s\n\n%s" % (e.response.status_code, e.response.url, '\n'.join(traceback.format_exception(*exc_info)))
    logging.warning(msg)

