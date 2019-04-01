from io import StringIO
from django.core.management import call_command
from django.test import TestCase
import responses
from unittest import mock
from website.management.commands import _gbif_api
from django.core import mail
from io import StringIO
from django.db import connection
import datetime

class PopulateResolverTest(TestCase):
    GBIF_API_DATASET_URL = "https://api.gbif.org/v1/dataset/{}"
    endpoints_example = [{'type': 'DWC_ARCHIVE', 'url': 'http://data.gbif.no/archive.do?r=dataset'}]

    @responses.activate
    def test_bad_core_email_alert(self):
        self._mock_get_dataset_list()
        self._mock_get_dataset_endpoints()
        with mock.patch('website.management.commands._gbif_api.get_cores_from_ipt', return_value=[('incorrect_core_type', StringIO('file_obj'))]):
            call_command('populate_resolver', stdout=StringIO())
        self.assertEqual(len(mail.outbox), 2)
        self.assertEqual(mail.outbox[0].subject, "[Django] Core type not supported: incorrect_core_type")
        self.assertIn(mail.outbox[0].body, "File url: http://data.gbif.no/archive.do?r=dataset")
        self.assertEqual(mail.outbox[1].subject, '[Django] Resolver import complete %s' % datetime.datetime.now().strftime("%Y-%m-%d"))
        self.assertIn(mail.outbox[1].body, 'Total number of rows imported 0')

    @responses.activate
    def test_bad_core_still_adds_other_valid_cores(self):
        self._mock_get_dataset_list()
        self._mock_get_dataset_endpoints()
        with open('website/tests/occurrence_test_file_small.txt') as file_obj:
            cores = [('incorrect_core_type', StringIO('file_obj')), ('occurrence', file_obj)]
            with mock.patch('website.management.commands._gbif_api.get_cores_from_ipt', return_value=cores):
                call_command('populate_resolver', stdout=StringIO())

        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM replacement_table")
            self.assertEqual(cursor.fetchone()[0], 5000)

    def _mock_get_dataset_list(self):
        mock_datasets = [{'key': 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649'}]
        mock_json = {'offset': 0, 'limit': 200, 'endOfRecords': 1, 'count': 200, 'results': mock_datasets}
        api_url = self.GBIF_API_DATASET_URL.format('search?limit=5000&publishingCountry=NO')
        responses.add(responses.GET, api_url, json=mock_json, status=200)

    def _mock_get_dataset_endpoints(self):
        url = self.GBIF_API_DATASET_URL.format('d34ed8a4-d3cb-473c-a11c-79c5fec4d649')
        responses.add(responses.GET, url, json={'key':'d34ed8a4-d3cb-473c-a11c-79c5fec4d649', 'endpoints': self.endpoints_example}, status=200)

