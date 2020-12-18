from io import StringIO
from django.core.management import call_command
import responses
from unittest import mock
from django.test import TestCase
from populator.models import Statistic, ResolvableObject
from website.models import Dataset


class PopulateResolverTest(TestCase):
    GBIF_API_DATASET_URL = "https://api.gbif.org/v1/dataset/{}"
    endpoints_example = [{'type': 'DWC_ARCHIVE', 'url': 'http://data.gbif.no/archive.do?r=dataset'}]
    SMALL_TEST_FILE = 'populator/tests/mock_data/dwca-seabird_estimates-v1.0.zip'
    SMALL_TEST_FILE_B = 'populator/tests/mock_data/dwca-molltax-v1.195.zip'

    @responses.activate
    def test_populate_resolver_adds_dataset_records_to_resolver(self):
        self.assertEqual(ResolvableObject.objects.count(), 0)
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with open(self.SMALL_TEST_FILE, 'rb') as dwc_zip_stream:
            responses.add(responses.GET, self.endpoints_example[0]['url'], body=dwc_zip_stream.read(), status=200,
                          content_type='application/zip', stream=True)
        call_command('populate_resolver', stdout=StringIO())
        self.assertEqual(ResolvableObject.objects.count(), 20191)

    @responses.activate
    def test_populate_resolver_adds_dataset(self):
        self.assertEqual(Dataset.objects.count(), 0)
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with open(self.SMALL_TEST_FILE, 'rb') as dwc_zip_stream:
            responses.add(responses.GET, self.endpoints_example[0]['url'], body=dwc_zip_stream.read(), status=200, content_type='application/zip', stream=True)
        call_command('populate_resolver', stdout=StringIO())
        self.assertEqual(Dataset.objects.count(), 1)

    @responses.activate
    def test_populate_resolver_updates_dataset(self):
        Dataset.objects.get_or_create(id='d34ed8a4-d3cb-473c-a11c-79c5fec4d649', data={})
        self.assertEqual(Dataset.objects.count(), 1)
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with open(self.SMALL_TEST_FILE, 'rb') as dwc_zip_stream:
            responses.add(responses.GET, self.endpoints_example[0]['url'], body=dwc_zip_stream.read(), status=200, content_type='application/zip', stream=True)
        call_command('populate_resolver', stdout=StringIO())
        self.assertEqual(Dataset.objects.count(), 1)
        self.assertEqual(Dataset.objects.all().first().id, 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649')

    @responses.activate
    def test_adds_total_count_to_website_statistics(self):
        self.assertEqual(ResolvableObject.objects.count(), 0)
        self.assertEqual(Statistic.objects.count(), 0)
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with open(self.SMALL_TEST_FILE, 'rb') as dwc_zip_stream:
            responses.add(responses.GET, self.endpoints_example[0]['url'], body=dwc_zip_stream.read(), status=200, content_type='application/zip', stream=True)
        call_command('populate_resolver', stdout=StringIO())
        self.assertEqual(Statistic.objects.get_total_count(), 20191)

    @responses.activate
    def test_adds_total_count_to_website_statistics_b(self):
        self.assertEqual(ResolvableObject.objects.count(), 0)
        self.assertEqual(Statistic.objects.count(), 0)

        mock_datasets = [{'title': 'A', 'doi': 'doi:mine', 'key': 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649'},
                         {'title': 'B', 'doi': 'doi:mine', 'key': 'a34ed8a4-d3cb-473c-a11c-79c5fec4d640'}]
        mock_json = {'offset': 0, 'limit': 200, 'endOfRecords': 1, 'count': 200, 'results': mock_datasets}
        api_url = self.GBIF_API_DATASET_URL.format('search?limit=5000&publishingCountry=NO')
        responses.add(responses.GET, api_url, json=mock_json, status=200)

        url = self.GBIF_API_DATASET_URL.format('d34ed8a4-d3cb-473c-a11c-79c5fec4d649')
        json_ = {'key': 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649', 'endpoints': self.endpoints_example, 'title': "A", 'doi': 'https://purl.org/my-dataset-doi'}
        responses.add(responses.GET, url, json=json_, status=200)

        url = self.GBIF_API_DATASET_URL.format('a34ed8a4-d3cb-473c-a11c-79c5fec4d640')
        new_endpoint = [{'type': 'DWC_ARCHIVE', 'url': 'http://data.gbif.no/archive.do?r=datasetb'}]
        json_ = {'key': 'a34ed8a4-d3cb-473c-a11c-79c5fec4d640', 'endpoints': new_endpoint, 'title': "B", 'doi': 'https://purl.org/my-dataset-doi'}
        responses.add(responses.GET, url, json=json_, status=200)

        with open(self.SMALL_TEST_FILE, 'rb') as dwc_zip_stream:
            responses.add(responses.GET, self.endpoints_example[0]['url'], body=dwc_zip_stream.read(), status=200, content_type='application/zip', stream=True)
        with open(self.SMALL_TEST_FILE_B, 'rb') as dwc_zip_stream:
            responses.add(responses.GET, new_endpoint[0]['url'], body=dwc_zip_stream.read(), status=200, content_type='application/zip', stream=True)
        call_command('populate_resolver', stdout=StringIO())
        self.assertEqual(Statistic.objects.get_total_count(), 20191 + 23227)

    @responses.activate
    def test_still_adds_records_for_other_valid_cores_with_unsupported_core(self):
        pass

    @responses.activate
    def test_skips_metadata_only_endpoints(self):
        self.assertEqual(ResolvableObject.objects.count(), 0)
        self._mock_get_dataset_list()
        url = self.GBIF_API_DATASET_URL.format('d34ed8a4-d3cb-473c-a11c-79c5fec4d649')
        endpoints_example = [{'type': 'EML', 'url': 'http://'}, {'type': 'EML_2', 'url': 'http://'}]
        responses.add(responses.GET, url, json={'key':'d34ed8a4-d3cb-473c-a11c-79c5fec4d649', 'endpoints': endpoints_example, 'title': 'My metadataset title', 'doi': 'https://purl.org/my-metadataset-doi', 'type': 'METADATA'}, status=200)
        call_command('populate_resolver', stdout=StringIO())
        self.assertEqual(ResolvableObject.objects.count(), 0)

    @responses.activate
    def _logs_while_adding_dataset_to_resolver(self):
        return # Disable logging for the moment
        self.assertEqual(ResolvableObject.objects.count(), 0)
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with open(self.SMALL_TEST_FILE, 'rb') as dwc_zip_stream:
              responses.add(responses.GET, self.endpoints_example[0]['url'], body=dwc_zip_stream.read(), status=200, content_type='application/zip', stream=True)
        with self.assertLogs() as cm:
            call_command('populate_resolver', stdout=StringIO())
            self.assertEqual(cm.output, ['INFO:root:Resolver import started', 'INFO:root:5000 items added for occurrence - http://data.gbif.no/archive.do?r=dataset',  'INFO:root:Resolver import complete: total number of rows imported 5000'])

    @responses.activate
    def _logs_error_if_bad_core(self):
        return
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with mock.patch('populator.management.commands._gbif_api.get_cores_from_ipt', return_value=[('incorrect_core_type', StringIO('file_obj'))]):
            with self.assertLogs() as cm:
                call_command('populate_resolver', stdout=StringIO())
                self.assertEqual(cm.output, ['INFO:root:Resolver import started', 'WARNING:root:Core type not supported: incorrect_core_type - http://data.gbif.no/archive.do?r=dataset', 'INFO:root:Resolver import complete: total number of rows imported 0'])

    @responses.activate
    def _no_records_added_logs_error(self):
        return
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with mock.patch('populator.management.commands._gbif_api.get_cores_from_ipt', return_value=[('occurrence', StringIO('file_obj'))]):
            with mock.patch('darwin_core_processing.copy_csv_to_populator_resolvableobjectmigration', return_value=0):
                with self.assertLogs() as cm:
                    call_command('populate_resolver', stdout=StringIO())
                    self.assertEqual(cm.output, ['INFO:root:Resolver import started', 'WARNING:root:No items added for occurrence - %s' % self.endpoints_example[0]['url'], 'INFO:root:Resolver import complete: total number of rows imported 0'])

    def _mock_get_dataset_list(self):  # Mocks out the call to the GBIF api to get a list of datasets
        mock_datasets = [{'title': 'My dataset', 'doi': 'doi:mine', 'comments': 'long comment', 'key': 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649'}]
        mock_json = {'offset': 0, 'limit': 200, 'endOfRecords': 1, 'count': 200, 'results': mock_datasets}
        api_url = self.GBIF_API_DATASET_URL.format('search?limit=5000&publishingCountry=NO')
        responses.add(responses.GET, api_url, json=mock_json, status=200)

    def _mock_get_dataset_detailed_info(self):
        url = self.GBIF_API_DATASET_URL.format('d34ed8a4-d3cb-473c-a11c-79c5fec4d649')
        responses.add(responses.GET, url, json={'key': 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649', 'endpoints': self.endpoints_example, 'title': "My dataset title with an apostrophe ' in it", 'doi': 'https://purl.org/my-dataset-doi'}, status=200)

