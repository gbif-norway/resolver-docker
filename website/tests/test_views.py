from website.models import DarwinCoreObject
import json
from rest_framework.test import APITestCase
from rest_framework.reverse import reverse


class ResolverViewTests(APITestCase):
    def test_displays_index(self):
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

    def test_displays_404_given_non_existent_uuid(self):
        response = self.client.get(reverse('darwincoreobject-detail', ['00000000-0000-0000-0000-000000000000']))
        self.assertTrue(response.status_code == 404)

    def test_renders_occurrence_json_ld(self):
        response_string = self._simple_request_occurrence('application/ld+json')
        expected_response = {'owl:sameas': 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580', '@id': 'http://purl.org/gbifnorway/id/5c0884ce-608c-4716-ba0e-cb389dca5580', 'dwc:basisofrecord': 'preservedspecimen', '@context': {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/', 'owl': 'https://www.w3.org/tr/owl-ref/'}}
        self.assertEqual(expected_response, json.loads(response_string))

    def test_renders_dataset_json_ld(self):
        response_string = self._simple_request_dataset('application/ld+json')
        expected_response = {'dwc:type': 'dataset', 'owl:sameas': 'https://doi.org/10.12345/abcdef', 'label': 'My dataset name' , '@id': 'http://purl.org/gbifnorway/id/5c0884ce-608c-4716-ba0e-cb389dca5580', '@context': {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/', 'owl': 'https://www.w3.org/TR/owl-ref/'}}

    def test_renders_occurrence_rdf(self):
        response_string = self._simple_request_occurrence('application/rdf+xml')
        self.assertTrue('<owl:sameas>urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580</owl:sameas>' in response_string)

    def test_renders_dataset_rdf(self):
        response_string = self._simple_request_dataset('application/rdf+xml')
        self.assertTrue('<owl:sameas>https://doi.org/https://doi.org/10.12345/abcdef</owl:sameas>' in response_string)

    def _simple_request_occurrence(self, http_accept):
        uuid = '5c0884ce-608c-4716-ba0e-cb389dca5580'
        DarwinCoreObject.objects.create(uuid=uuid, data={'id': uuid, 'basisOfRecord': 'preservedspecimen'})
        url = reverse('darwincoreobject-detail', [uuid])
        response = self.client.get(url, HTTP_ACCEPT=http_accept)
        return response.content.decode('utf-8').lower()

    def _simple_request_dataset(self, http_accept):
        uuid = '5c0884ce-608c-4716-ba0e-cb389dca5580'
        DarwinCoreObject.objects.create(uuid=uuid, data={'label': 'My dataset name', 'type': 'dataset', 'sameas': 'https://doi.org/10.12345/abcdef'})
        url = reverse('darwincoreobject-detail', [uuid])
        response = self.client.get(url, HTTP_ACCEPT=http_accept)
        return response.content.decode('utf-8').lower()

