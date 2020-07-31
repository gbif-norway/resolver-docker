import datetime
from io import StringIO
import psycopg2 as p
import responses
from unittest import TestCase, mock
from zipfile import ZipFile
import populate_resolver
from datetime import date


class PopulateResolverTest(TestCase):
    GBIF_API_DATASET_URL = "https://api.gbif.org/v1/dataset/{}"
    endpoints_example = [{'type': 'DWC_ARCHIVE', 'url': 'http://data.gbif.no/archive.do?r=dataset'}]

    def setUp(self):
        with open('tests/create_db.sql', 'r') as reader:
            create_sql = reader.read()
        with p.connect('') as conn:
            with conn.cursor() as cursor:
                cursor.execute('DROP TABLE IF EXISTS replacement_table, website_history, website_darwincoreobject, website_statistic')
                cursor.execute(create_sql)
                cursor.execute('CREATE TABLE replacement_table (LIKE website_darwincoreobject INCLUDING ALL)')

    def tearDown(self):
         with p.connect('') as conn:
             with conn.cursor() as cursor:
                 cursor.execute('DROP TABLE IF EXISTS replacement_table, website_history, website_darwincoreobject, website_statistic')

    @responses.activate
    def test_populate_resolver_adds_dataset_to_resolver(self):
        self.assertEqual(get_darwin_core_object_count(), 0)
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with open('tests/mock_data/occurrence_test_file_small.txt.zip', 'rb') as dwc_zip_stream:
            responses.add(responses.GET, self.endpoints_example[0]['url'], body=dwc_zip_stream.read(), status=200, content_type='application/zip', stream=True)
        populate_resolver.populate_resolver()
        self.assertEqual(get_darwin_core_object_count(), 5001)

    @responses.activate
    def test_adds_total_count_to_website_statistics(self):
        self.assertEqual(get_darwin_core_object_count(), 0)
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with open('tests/mock_data/occurrence_test_file_small.txt.zip', 'rb') as dwc_zip_stream:
            responses.add(responses.GET, self.endpoints_example[0]['url'], body=dwc_zip_stream.read(), status=200, content_type='application/zip', stream=True)
        populate_resolver.populate_resolver()
        with p.connect('') as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT value FROM website_statistic")
                total_count = cursor.fetchone()
        self.assertEqual(total_count[0], 5001)

    @responses.activate
    def test_logs_while_adding_dataset_to_resolver(self):
        self.assertEqual(get_darwin_core_object_count(), 0)
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with open('tests/mock_data/occurrence_test_file_small.txt.zip', 'rb') as dwc_zip_stream:
              responses.add(responses.GET, self.endpoints_example[0]['url'], body=dwc_zip_stream.read(), status=200, content_type='application/zip', stream=True)
        with self.assertLogs() as cm:
            populate_resolver.populate_resolver()
            self.assertEqual(cm.output, ['INFO:root:Resolver import started', 'INFO:root:5000 items added for occurrence - http://data.gbif.no/archive.do?r=dataset',  'INFO:root:Resolver import complete: total number of rows imported 5000'])

    @responses.activate
    def test_logs_error_if_bad_core(self):
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with mock.patch('gbif_api.get_cores_from_ipt', return_value=[('incorrect_core_type', StringIO('file_obj'))]):
            with self.assertLogs() as cm:
                populate_resolver.populate_resolver()
                self.assertEqual(cm.output, ['INFO:root:Resolver import started', 'WARNING:root:Core type not supported: incorrect_core_type - http://data.gbif.no/archive.do?r=dataset', 'INFO:root:Resolver import complete: total number of rows imported 0'])

    @responses.activate
    def test_still_adds_records_for_other_valid_cores_with_bad_core(self):
        self.assertEqual(get_darwin_core_object_count(), 0)
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with ZipFile('tests/mock_data/occurrence_test_file_small.txt.zip', 'r') as file_obj:
            cores = [('incorrect_core_type', StringIO('file_obj')), ('occurrence', file_obj.open('occurrence.txt'))]
            with mock.patch('gbif_api.get_cores_from_ipt', return_value=cores):
                populate_resolver.populate_resolver()
        self.assertEqual(get_darwin_core_object_count(), 5001)

    @responses.activate
    def test_skips_metadata_only_endpoints(self):
        self.assertEqual(get_darwin_core_object_count(), 0)
        self._mock_get_dataset_list()
        url = self.GBIF_API_DATASET_URL.format('d34ed8a4-d3cb-473c-a11c-79c5fec4d649')
        endpoints_example = [{'type': 'EML', 'url': 'http://'}, {'type': 'EML_2', 'url': 'http://'}]
        responses.add(responses.GET, url, json={'key':'d34ed8a4-d3cb-473c-a11c-79c5fec4d649', 'endpoints': endpoints_example, 'title': 'My metadataset title', 'doi': 'https://purl.org/my-metadataset-doi', 'type': 'dataset'}, status=200)
        populate_resolver.populate_resolver()
        self.assertEqual(get_darwin_core_object_count(), 1)

    @responses.activate
    def test_no_records_added_logs_error(self):
        self._mock_get_dataset_list()
        self._mock_get_dataset_detailed_info()
        with mock.patch('gbif_api.get_cores_from_ipt', return_value=[('occurrence', StringIO('file_obj'))]):
            with mock.patch('darwin_core_processing.copy_csv_to_replacement_table', return_value=0):
                with self.assertLogs() as cm:
                    populate_resolver.populate_resolver()
                    self.assertEqual(cm.output, ['INFO:root:Resolver import started', 'WARNING:root:No items added for occurrence - %s' % self.endpoints_example[0]['url'], 'INFO:root:Resolver import complete: total number of rows imported 0'])

    def _mock_get_dataset_list(self): # Mocks out the call to the GBIF api to get a list of datasets
        mock_datasets = [{'key': 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649'}]
        mock_json = {'offset': 0, 'limit': 200, 'endOfRecords': 1, 'count': 200, 'results': mock_datasets}
        api_url = self.GBIF_API_DATASET_URL.format('search?limit=5000&publishingCountry=NO')
        responses.add(responses.GET, api_url, json=mock_json, status=200)

    def _mock_get_dataset_detailed_info(self):
        url = self.GBIF_API_DATASET_URL.format('d34ed8a4-d3cb-473c-a11c-79c5fec4d649')
        responses.add(responses.GET, url, json={'key':'d34ed8a4-d3cb-473c-a11c-79c5fec4d649', 'endpoints': self.endpoints_example, 'title': "My dataset title with an apostrophe ' in it", 'doi': 'https://purl.org/my-dataset-doi'}, status=200)

def get_darwin_core_object_count():
    with p.connect('') as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM website_darwincoreobject')
            return cursor.fetchone()[0]

if __name__ == '__main__':
    unittest.main()
