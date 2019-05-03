from django.test import TestCase
from website.models import DarwinCoreObject
from django.urls import reverse
import json

class ResolverViewTests(TestCase):

    def test_displays_index(self):
        response = self.client.get(reverse('index'))
        self.assertEqual(response.status_code, 200)

    def test_displays_data_given_valid_uuid(self):
        response_string = self._simple_request('text/html')
        self.assertTrue('occurrence' in response_string)
        self.assertTrue('5c0884ce-608c-4716-ba0e-cb389dca5580' in response_string)

    def test_displays_404_given_invalid_uuid(self):
        response = self.client.get(reverse('detail', args=['00000000-0000-0000-0000-000000000000']))
        self.assertContains(response, 'not found', status_code=404)

    def test_renders_json(self):
        response_string = self._simple_request('application/json')
        expected_response = {'dwc:type': 'occurrence', 'dwc:uuid': '5c0884ce-608c-4716-ba0e-cb389dca5580', '@id': 'http://purl.org/gbifnorway/id/5c0884ce-608c-4716-ba0e-cb389dca5580', '@context': {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/'}}
        self.assertEqual(expected_response, json.loads(response_string))

    def test_renders_n3(self):
        response_string = self._simple_request('text/n3')
        self.assertTrue('<dwc:type>occurrence</dwc:type>' in response_string)
        self.assertTrue('<dwc:uuid>5c0884ce-608c-4716-ba0e-cb389dca5580</dwc:uuid>' in response_string)

    def test_renders_xml(self):
        response_string = self._simple_request('application/xml')
        self.assertTrue('<dwc:type>occurrence</dwc:type>' in response_string)
        self.assertTrue('<dwc:uuid>5c0884ce-608c-4716-ba0e-cb389dca5580</dwc:uuid>' in response_string)

    def _simple_request(self, http_accept):
        uuid = '5c0884ce-608c-4716-ba0e-cb389dca5580'
        obj_1 = DarwinCoreObject.objects.create(uuid=uuid, data={'uuid': uuid, 'type': 'occurrence'})
        response = self.client.get(reverse('detail', args=[uuid]), HTTP_ACCEPT=http_accept)
        return response.content.decode('utf-8').lower()

