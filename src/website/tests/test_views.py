from website.models import ResolvableObject, Dataset
from populator.models import Statistic, History
import json
import requests
from unittest import mock
from rest_framework.test import APITestCase
from rest_framework.reverse import reverse
from django.test import SimpleTestCase
from django.db import DatabaseError


class LivenessViewTests(SimpleTestCase):
    def test_liveness_does_not_need_database(self):
        response = self.client.get(reverse('healthz'))
        self.assertEqual(response.status_code, 200)


class ReadinessViewTests(APITestCase):
    def test_ready_when_database_is_available(self):
        response = self.client.get(reverse('readyz'))
        self.assertEqual(response.status_code, 200)

    @mock.patch('website.views.connection.cursor', side_effect=DatabaseError('unavailable'))
    def test_not_ready_when_database_is_unavailable(self, cursor_mock):
        response = self.client.get(reverse('readyz'))
        self.assertEqual(response.status_code, 503)


class HistoryViewTests(APITestCase):
    def setUp(self):
        self.dataset = Dataset.objects.create(id='dataset_id', data={'label': 'My dataset', 'type': 'event'})
        self.resolvable_object = ResolvableObject.objects.create(id='a', data={'test': 'a'}, dataset=self.dataset)
        History.objects.create(resolvable_object=self.resolvable_object, changed_data={'test': 'b'})

    def test_list(self):
        History.objects.create(resolvable_object=self.resolvable_object, changed_data={'test': 'c'})
        response = self.client.get(reverse('history-list'), HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 2)

    def test_invalid_json_filter_returns_400(self):
        response = self.client.get(reverse('history-list') + '?changed_data__contains=not-json')
        self.assertEqual(response.status_code, 400)

    def test_filters(self):
        new = ResolvableObject.objects.create(id='b', data={'testb': 'a'}, dataset=self.dataset)
        History.objects.create(resolvable_object=new, changed_data={'testb': 'b'})
        response = self.client.get(reverse('history-list') + '?resolvable_object=b', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 1)

    def test_changed_data_contains_single_filter(self):
        History.objects.create(resolvable_object=self.resolvable_object, changed_data={'test': 'c'})
        response = self.client.get(reverse('history-list') + '?changed_data__contains={"test":"b"}', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 1)
        self.assertEqual(results['results'][0]['changed_data'], { 'test': 'b' })

    def test_changed_data_contains_multiple_filter(self):
        History.objects.create(resolvable_object=self.resolvable_object, changed_data={'test': 'c', 'new': 'd'})
        response = self.client.get(reverse('history-list') + '?changed_data__contains={"test":"c", "new": "d"}', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 1)
        self.assertEqual(results['results'][0]['changed_data'], { 'test': 'c', 'new': 'd' })

    def test_changed_data_has_key_filter(self):
        History.objects.create(resolvable_object=self.resolvable_object, changed_data={'newkey': 'new'})
        response = self.client.get(reverse('history-list') + '?changed_data__has_key=test', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 1)
        self.assertEqual(results['results'][0]['changed_data'], { 'test': 'b' })

    def test_combined_filters(self):
        History.objects.create(resolvable_object=self.resolvable_object, changed_data={'filterx': 'x', 'filtery': 'y', 'newkey': '-'})
        new = ResolvableObject.objects.create(id='b', data={}, dataset=self.dataset)
        History.objects.create(resolvable_object=new, changed_data={'filterx': 'x', 'filtery': 'y'})
        History.objects.create(resolvable_object=new, changed_data={'filterx': 'x', 'filtery': 'y', 'newkey': '-', 'anotherkey': '-'})
        filters = '&'.join(['resolvable_object=b', 'changed_data__contains={"filterx": "x", "filtery": "y"}', 'changed_data__has_key=newkey'])
        response = self.client.get(reverse('history-list') + '?' + filters, HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 1)
        print(results['results'][0])
        self.assertEqual(results['results'][0]['changed_data'], { 'newkey': '-', 'filterx': 'x', 'filtery': 'y', 'anotherkey': '-' })


class ResolverViewTests(APITestCase):
    def setUp(self):
        self.dataset = Dataset.objects.create(id='a', data={'label': 'My dataset', 'type': 'event'})

    def test_displays_index(self):
        Statistic.objects.set_total_count()
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

    def test_displays_404_given_non_existent_id(self):
        response = self.client.get(reverse('resolvableobject-detail', ['00000000-0000-0000-0000-000000000000']))
        self.assertTrue(response.status_code == 404)

    def test_filters_do_not_break_with_paginator(self):
        Statistic.objects.set_total_count()
        response = self.client.get(reverse('resolvableobject-list') + '?offset=10&limit=20', HTTP_ACCEPT='application/ld+json')
        self.assertEqual(response.status_code, 200)

    def test_displays_all_results(self):
        for item in 'abcde':
            ResolvableObject.objects.create(id=item, data={'test': item}, dataset=self.dataset)
        Statistic.objects.set_total_count()  # Total count must be manually pre-set when database is populated to return results here, too slow to calculate on the fly
        response = self.client.get(reverse('resolvableobject-list') + '?limit=10', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 5)

    def test_pagination(self):
        url = reverse('resolvableobject-list')
        for item in 'abcde':
            ResolvableObject.objects.create(id=item, data={'test': item}, dataset=self.dataset)
        Statistic.objects.create(name='total_count', value=5)
        response = self.client.get(url + '?offset=3&limit=2', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 2)
        self.assertEqual(results['results'][0]['dwc:test'], 'd')
        self.assertEqual(results['results'][1]['dwc:test'], 'e')

    def test_filters_with_pagination(self):
        for item in [('a', 'Galium',), ('b', 'Eudyptes'), ('c', 'Eudyptes'), ('d', 'Galium'), ('e', 'Eudyptes')]:
            ResolvableObject.objects.create(id=item[0], data={'id': item[0], 'scientificname': item[1]}, dataset=self.dataset)
        response = self.client.get(reverse('resolvableobject-list') + '?offset=1&limit=1&scientificname=Eudyptes', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 1)
        self.assertEqual(results['results'][0]['dwc:scientificname'], 'eudyptes')
        self.assertEqual(results['results'][0]['owl:sameas'], 'c')

    def test_correct_count_with_filtering_and_pagination(self):
        for item in [('a', 'Galium',), ('b', 'Eudyptes'), ('c', 'Eudyptes'), ('d', 'Galium'), ('e', 'Eudyptes')]:
            ResolvableObject.objects.create(id=item[0], data={'id': item[0], 'scientificname': item[1]}, dataset=self.dataset)
        response = self.client.get(reverse('resolvableobject-list') + '?offset=1&limit=1&scientificname=Eudyptes&_add_counts=true', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(results['count'], 3)

    def test_filters_on_scientific_name(self):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Galium odoratum'})
        id = 'urn:uuid:6c0884ce-608c-4716-ba0e-cb389dca5581'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})

        url = reverse('resolvableobject-list')
        response = self.client.get(url + '?scientificname=Galium%20odoratum', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 1)
        self.assertEqual(results['results'][0]['dwc:scientificname'], 'galium odoratum')

    def test_filters_on_dataset_id(self):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Galium odoratum'})
        new_dataset = Dataset.objects.create(id='b', data={'label': 'New dataset', 'type': 'occurrence'})
        id = 'urn:uuid:6c0884ce-608c-4716-ba0e-cb389dca5581'
        ResolvableObject.objects.create(id=id, dataset=new_dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})

        url = reverse('resolvableobject-list')
        response = self.client.get(url + '?dataset_id=b', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 1)
        self.assertEqual(results['results'][0]['dc:ispartof']['id'], 'https://gbif.org/dataset/b')

    def test_filters_on_multiple(self):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Galium odoratum'})
        id = 'urn:uuid:6c0884ce-608c-4716-ba0e-cb389dca5581'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})
        id = 'urn:uuid:7c0884ce-608c-4716-ba0e-cb389dca5582'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})

        url = reverse('resolvableobject-list')
        response = self.client.get(url + '?scientificname=Eudyptes%20moseleyi', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(results['results'][0]['dwc:scientificname'], 'eudyptes moseleyi')
        self.assertEqual(results['results'][1]['dwc:scientificname'], 'eudyptes moseleyi')

    def test_calculates_correct_counts_with_filter(self):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Galium odoratum'})
        id = 'urn:uuid:6c0884ce-608c-4716-ba0e-cb389dca5581'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})
        id = 'urn:uuid:7c0884ce-608c-4716-ba0e-cb389dca5582'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})

        url = reverse('resolvableobject-list')
        response = self.client.get(url + '?scientificname=Eudyptes%20moseleyi&_add_counts=true', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 2)
        self.assertEqual(results['count'], 2)

    def test_calculates_correct_counts_without_filter(self): # FAILS
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Galium odoratum'})
        id = 'urn:uuid:6c0884ce-608c-4716-ba0e-cb389dca5581'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})
        id = 'urn:uuid:7c0884ce-608c-4716-ba0e-cb389dca5582'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen', 'scientificname': 'Eudyptes moseleyi'})

        url = reverse('resolvableobject-list')
        response = self.client.get(url + '?_add_counts=true', HTTP_ACCEPT='application/ld+json')
        results = json.loads(response.content.decode('utf-8').lower())
        self.assertEqual(len(results['results']), 3)
        self.assertEqual(results['count'], 3) # Note: this fails at the moment until I can figure out a better way to count

    # def test_renders_occurrence_json_ld(self):
    #     response_string = self._simple_request_occurrence('application/ld+json')
    #     expected_response = {'owl:sameas': 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580',
    #                          '@id': 'http://purl.org/gbifnorway/id/urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580',
    #                          'dwc:basisofrecord': 'preservedspecimen',
    #                          'data-type': '',
    #                          'deleted_date': None,
    #                          'dataset': {'id': 'a', 'label': 'my dataset', 'type': 'event'},
    #                          '@context': {'dc': 'http://purl.org/dc/elements/1.1/',
    #                                       'dwc': 'http://rs.tdwg.org/dwc/terms/',
    #                                       'owl': 'https://www.w3.org/tr/owl-ref/'}}
    #     self.assertEqual(expected_response, json.loads(response_string))
    #
    # def test_renders_dataset_json_ld(self):
    #     response_string = self._simple_request_dataset('application/ld+json')
    #     expected_response = {'rdf:type': 'dataset',
    #                          'owl:sameas': 'https://doi.org/10.12345/abcdef',
    #                          'rdfs:label': 'my dataset name' ,
    #                          '@id': 'http://purl.org/gbifnorway/id/urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580',
    #                          'data-type': '',
    #                          'deleted_date': None,
    #                          'dataset': {'id': 'a', 'label': 'my dataset', 'type': 'event'},
    #                          '@context': {'dc': 'http://purl.org/dc/elements/1.1/',
    #                                       'dwc': 'http://rs.tdwg.org/dwc/terms/',
    #                                       'owl': 'https://www.w3.org/tr/owl-ref/',
    #                                       'rdfs': 'https://www.w3.org/tr/rdf-schema/',
    #                                       'rdf': 'https://www.w3.org/tr/rdf-schema/'}}
    #     self.assertEqual(expected_response, json.loads(response_string))
    #
    # def test_renders_occurrence_rdf(self):
    #     response_string = self._simple_request_occurrence('application/rdf+xml')
    #     self.assertTrue('<owl:sameas>urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580</owl:sameas>' in response_string)
    #
    # def test_renders_dataset_rdf(self):
    #     response_string = self._simple_request_dataset('application/rdf+xml')
    #     self.assertTrue('<owl:sameas>https://doi.org/10.12345/abcdef</owl:sameas>' in response_string)

    def test_lists_specimens_sharing_a_uuid(self):
        uuid = '3136d80a-e74c-11e4-a2dc-00155d012a60'
        for catalognumber in ['1', '2']:
            ResolvableObject.objects.create(id=f'urn:catalog:o:v:{catalognumber}', dataset=self.dataset, data={'catalognumber': catalognumber, 'materialsampleid': uuid, 'basisofrecord': 'Preservedspecimen'})
        url = reverse('resolvableobject-detail', [uuid.upper()])
        response = self.client.get(url, HTTP_ACCEPT='application/ld+json')
        self.assertEqual(response.status_code, 300)
        results = json.loads(response.content.decode('utf-8'))['results']
        self.assertEqual([result['@id'] for result in results], ['http://purl.org/gbifnorway/id/urn:catalog:o:v:1', 'http://purl.org/gbifnorway/id/urn:catalog:o:v:2'])
        self.assertEqual(self.client.get(url, HTTP_ACCEPT='text/html').status_code, 300)

    def test_resolves_uuid_found_only_in_materialsampleid(self):
        uuid = '3136d80a-e74c-11e4-a2dc-00155d012a60'
        ResolvableObject.objects.create(id='urn:catalog:o:v:1', dataset=self.dataset, data={'catalognumber': '1', 'materialsampleid': uuid.upper(), 'basisofrecord': 'Preservedspecimen'})
        response = self.client.get(reverse('resolvableobject-detail', [f'urn:uuid:{uuid}']), HTTP_ACCEPT='application/ld+json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content.decode('utf-8'))['dwc:catalognumber'], '1')

    def test_displays_404_given_id_that_is_not_a_uuid(self):
        ResolvableObject.objects.create(id='urn:catalog:o:v:1', dataset=self.dataset, data={'materialsampleid': 'abc'})
        response = self.client.get(reverse('resolvableobject-detail', ['abc']))
        self.assertEqual(response.status_code, 404)

    def test_case_insensitive_id_resolving(self):
        id = 'ABC'
        ResolvableObject.objects.create(id='abc', dataset=self.dataset, data={'id': id.lower(), 'basisOfRecord': 'preservedspecimen'})
        url = reverse('resolvableobject-detail', [id])
        response = self.client.get(url, HTTP_ACCEPT='application/ld+json')
        self.assertTrue(response.status_code == 200)

    def _simple_request_occurrence(self, http_accept):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'id': id, 'basisOfRecord': 'preservedspecimen'})
        url = reverse('resolvableobject-detail', [id])
        response = self.client.get(url, HTTP_ACCEPT=http_accept)
        return response.content.decode('utf-8').lower()

    def _simple_request_dataset(self, http_accept):
        id = 'urn:uuid:5c0884ce-608c-4716-ba0e-cb389dca5580'
        ResolvableObject.objects.create(id=id, dataset=self.dataset, data={'label': 'my dataset name', 'type': 'dataset', 'sameas': 'https://doi.org/10.12345/abcdef', 'id': id})
        url = reverse('resolvableobject-detail', [id])
        response = self.client.get(url, HTTP_ACCEPT=http_accept)
        return response.content.decode('utf-8').lower()


class FromGbifViewTests(APITestCase):
    @mock.patch('website.views.fetch_gbif_occurrence')
    def test_redirects_to_occurrence_id(self, fetch_mock):
        fetch_mock.return_value = {'occurrenceID': 'urn:catalog:O:F:256920'}
        response = self.client.get(reverse('from-gbif', kwargs={'gbif_key': '4429413305'}))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response['Location'], '/urn%3Acatalog%3AO%3AF%3A256920')

    @mock.patch('website.views.fetch_gbif_occurrence')
    def test_returns_404_when_key_not_found(self, fetch_mock):
        fetch_mock.return_value = None
        response = self.client.get(reverse('from-gbif', kwargs={'gbif_key': '4429413305'}))
        self.assertEqual(response.status_code, 404)

    @mock.patch('website.views.fetch_gbif_occurrence')
    def test_returns_422_when_occurrenceid_missing(self, fetch_mock):
        fetch_mock.return_value = {'key': 4429413305}
        response = self.client.get(reverse('from-gbif', kwargs={'gbif_key': '4429413305'}))
        self.assertEqual(response.status_code, 422)

    @mock.patch('website.views.fetch_gbif_occurrence')
    def test_returns_503_on_gbif_api_failure(self, fetch_mock):
        fetch_mock.side_effect = requests.exceptions.Timeout('boom')
        response = self.client.get(reverse('from-gbif', kwargs={'gbif_key': '4429413305'}))
        self.assertEqual(response.status_code, 503)
