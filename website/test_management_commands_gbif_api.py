from website.management.commands import _gbif_api
from django.test import TestCase
import responses
import requests

class GbifApiTest(TestCase):
    endpoints_example = [{'type': 'EML', 'url': 'http://data.gbif.no/eml.do?r=dataset'}, {'type': 'DWC_ARCHIVE', 'url': 'http://data.gbif.no/archive.do?r=dataset'}, {'type': 'DWC_ARCHIVE', 'url': 'old-and-invalid'}]

    def test_get_first_darwin_core_url_from_list(self):
        self.assertEqual(_gbif_api.get_first_darwin_core_url_from_list(self.endpoints_example), self.endpoints_example[1])

    @responses.activate
    def test_get_dataset_list(self):
        mock_datasets = [{'key': 'b124e1e0-4755-430f-9eab-894f25a9b59c'}, {'key': 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649'}, {'key': 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649'}]
        mock_json = {'offset': 0, 'limit': 200, 'endOfRecords': 1, 'count': 200, 'results': mock_datasets}
        api_url = _gbif_api.GBIF_API_DATASET_URL.format('search?limit=5000&publishingCountry=NO')
        responses.add(responses.GET, api_url, json=mock_json, status=200)  # A mock for the API call

        dataset_list = _gbif_api.get_dataset_list()
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url, api_url)
        self.assertEqual(dataset_list, mock_datasets)

    @responses.activate
    def test_get_dataset_endpoints(self):
        key = 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649'
        api_url = _gbif_api.GBIF_API_DATASET_URL.format(key)
        responses.add(responses.GET, api_url, json={'key': key, 'endpoints': self.endpoints_example}, status=200)

        dataset_endpoints = _gbif_api.get_dataset_endpoints(key)
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url, api_url)
        self.assertEqual(dataset_endpoints, self.endpoints_example)

