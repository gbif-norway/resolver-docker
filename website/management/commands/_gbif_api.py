from io import BytesIO
from zipfile import ZipFile
import requests

GBIF_API_DATASET_URL = "https://api.gbif.org/v1/dataset/{}"

def get_dataset_list():
    response = requests.get(GBIF_API_DATASET_URL.format('search?limit=5000&publishingCountry=NO'))
    json = response.json()
    return json['results']

def get_dataset_endpoints(dataset_key):
    response = requests.get(GBIF_API_DATASET_URL.format(dataset_key))
    json = response.json()
    return json['endpoints']

def get_first_darwin_core_url_from_list(endpoints):
    return next(endpoint for endpoint in endpoints if endpoint['type'] == 'DWC_ARCHIVE')

def get_cores_from_ipt(url):
    file_data = requests.get(url)
    zipfile = ZipFile(BytesIO(file_data.content))
    file_names = zipfile.namelist()

    # Returns e.g. [('occurrence', fileobj), ('multimedia', fileobj)] for occurrence.txt and multimedia.txt files
    return [(fn[:-3], zipfile.open(fn)) for fn in file_names if fn[-3:] == 'txt']

