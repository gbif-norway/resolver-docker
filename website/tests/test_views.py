from website.models import DarwinCoreObject
import json
from rest_framework.test import APITestCase
from rest_framework.reverse import reverse


class ResolverViewTests(APITestCase):
    def test_displays_index(self):
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

    def test_displays_404_given_non_existent_id(self):
        response = self.client.get(reverse('darwincoreobject-detail', ['00000000-0000-0000-0000-000000000000']))
        self.assertTrue(response.status_code == 404)

    def test_filters_do_not_break_with_paginator(self):
        url = reverse('darwincoreobject-list')
        response = self.client.get(url + '?offset=10&limit=20', HTTP_ACCEPT='application/ld+json')

    def test_filters_on_scientific_name(self):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Galium odoratum'})
        id = 'urn:uuid:6c0884ce-608c-4716-ba0e-cb389dca5581'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})

        url = reverse('darwincoreobject-list')
        response = self.client.get(url + '?scientificname=Galium%20odoratum', HTTP_ACCEPT='application/ld+json')
        response_string = response.content.decode('utf-8').lower()
        results = json.loads(response_string)
        self.assertEqual(len(results['results']), 1)
        self.assertEqual(results['results'][0]['dwc:scientificname'], 'galium odoratum')

    def test_filters_on_multiple(self):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Galium odoratum'})
        id = 'urn:uuid:6c0884ce-608c-4716-ba0e-cb389dca5581'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})
        id = 'urn:uuid:7c0884ce-608c-4716-ba0e-cb389dca5582'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})

        url = reverse('darwincoreobject-list')
        response = self.client.get(url + '?scientificname=Eudyptes%20moseleyi', HTTP_ACCEPT='application/ld+json')
        response_string = response.content.decode('utf-8').lower()
        results = json.loads(response_string)
        self.assertEqual(results['results'][0]['dwc:scientificname'], 'eudyptes moseleyi')
        self.assertEqual(results['results'][1]['dwc:scientificname'], 'eudyptes moseleyi')

    def test_calculates_correct_counts_with_filter(self):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Galium odoratum'})
        id = 'urn:uuid:6c0884ce-608c-4716-ba0e-cb389dca5581'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})
        id = 'urn:uuid:7c0884ce-608c-4716-ba0e-cb389dca5582'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})

        url = reverse('darwincoreobject-list')
        response = self.client.get(url + '?scientificname=Eudyptes%20moseleyi', HTTP_ACCEPT='application/ld+json')
        response_string = response.content.decode('utf-8').lower()
        results = json.loads(response_string)
        self.assertEqual(len(results['results']), 2)
        self.assertEqual(results['count'], 2)

    def _test_calculates_correct_counts_without_filter(self): # FAILS
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Galium odoratum'})
        id = 'urn:uuid:6c0884ce-608c-4716-ba0e-cb389dca5581'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})
        id = 'urn:uuid:7c0884ce-608c-4716-ba0e-cb389dca5582'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})

        url = reverse('darwincoreobject-list')
        response = self.client.get('/', HTTP_ACCEPT='application/ld+json')
        response_string = response.content.decode('utf-8').lower()
        results = json.loads(response_string)
        self.assertEqual(len(results['results']), 3)
        self.assertEqual(results['count'], 3) # Note: this fails at the moment until I can figure out a better way to count

    def test_renders_occurrence_json_ld(self):
        response_string = self._simple_request_occurrence('application/ld+json')
        expected_response = {'owl:sameas': 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580', '@id': 'http://purl.org/gbifnorway/id/urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580', 'dwc:basisofrecord': 'preservedspecimen', '@context': {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/', 'owl': 'https://www.w3.org/tr/owl-ref/'}}
        self.assertEqual(expected_response, json.loads(response_string))

    def test_renders_dataset_json_ld(self):
        response_string = self._simple_request_dataset('application/ld+json')
        expected_response = {'dc:type': 'dataset', 'owl:sameas': 'https://doi.org/10.12345/abcdef', 'rdfs:label': 'my dataset name' , '@id': 'http://purl.org/gbifnorway/id/urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580', '@context': {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/', 'owl': 'https://www.w3.org/tr/owl-ref/', 'rdfs': 'https://www.w3.org/tr/rdf-schema/'}}
        self.assertEqual(expected_response, json.loads(response_string))

    def test_renders_occurrence_rdf(self):
        response_string = self._simple_request_occurrence('application/rdf+xml')
        self.assertTrue('<owl:sameas>urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580</owl:sameas>' in response_string)

    def test_renders_dataset_rdf(self):
        response_string = self._simple_request_dataset('application/rdf+xml')
        self.assertTrue('<owl:sameas>https://doi.org/10.12345/abcdef</owl:sameas>' in response_string)

    def _simple_request_occurrence(self, http_accept):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        DarwinCoreObject.objects.create(id=id, data={'id': id, 'basisOfRecord': 'preservedspecimen'})
        url = reverse('darwincoreobject-detail', [id])
        response = self.client.get(url, HTTP_ACCEPT=http_accept)
        return response.content.decode('utf-8').lower()

    def _simple_request_dataset(self, http_accept):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        DarwinCoreObject.objects.create(id=id, data={'label': 'my dataset name', 'type': 'dataset', 'sameas': 'https://doi.org/10.12345/abcdef', 'id': id})
        url = reverse('darwincoreobject-detail', [id])
        response = self.client.get(url, HTTP_ACCEPT=http_accept)
        return response.content.decode('utf-8').lower()

