from website.models import DarwinCoreObject
import json
from rest_framework.test import APITestCase
from rest_framework.reverse import reverse


class ResolverViewTests(APITestCase):
    def test_displays_index(self):
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

    def test_displays_data_given_valid_uuid(self):
        response_string = self._simple_request('application/json')
        self.assertTrue('occurrence' in response_string)
        self.assertTrue('5c0884ce-608c-4716-ba0e-cb389dca5580' in response_string)

    def test_displays_404_given_invalid_uuid(self):
        response = self.client.get(reverse('darwincoreobject-detail', ['00000000-0000-0000-0000-000000000000']))
        self.assertTrue(response.status_code == 404)

    def test_renders_json_ld(self):
        response_string = self._simple_request('application/ld+json')
        expected_response = {'dwc:type': 'occurrence', 'dwc:id': '5c0884ce-608c-4716-ba0e-cb389dca5580', '@id': 'http://purl.org/gbifnorway/id/5c0884ce-608c-4716-ba0e-cb389dca5580', '@context': {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/'}}
        self.assertEqual(expected_response, json.loads(response_string))

    def test_renders_rdf(self):
        response_string = self._simple_request('application/rdf+xml')
        self.assertTrue('<dwc:type>occurrence</dwc:type>' in response_string)
        self.assertTrue('<dwc:id>5c0884ce-608c-4716-ba0e-cb389dca5580</dwc:id>' in response_string)

    def _simple_request(self, http_accept):
        uuid = '5c0884ce-608c-4716-ba0e-cb389dca5580'
        DarwinCoreObject.objects.create(uuid=uuid, data={'id': uuid, 'type': 'occurrence'})
        url = reverse('darwincoreobject-detail', [uuid])
        response = self.client.get(url, HTTP_ACCEPT=http_accept)
        return response.content.decode('utf-8').lower()

