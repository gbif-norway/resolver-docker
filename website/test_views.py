from django.test import TestCase #, Client
#from django.test.utils import setup_test_environment
from .models import DarwinCoreObject
from django.urls import reverse

class ResolverViewTests(TestCase):

    def test_displays_index(self):
        response = self.client.get(reverse('index'))
        self.assertEqual(response.status_code, 200)

    def test_displays_data_given_valid_uuid(self):
        uuid = '5c0884ce-608c-4716-ba0e-cb389dca5580'
        obj_1 = DarwinCoreObject.objects.create(uuid=uuid, data={'id': uuid, 'type': 'occurrence'})
        response = self.client.get(reverse('detail', args=[uuid]))
        response_string = response.content.decode('utf-8').lower()
        self.assertTrue('occurrence' in response_string)
        self.assertTrue(uuid in response_string)

    def test_displays_404_given_invalid_uuid(self):
        response = self.client.get(reverse('detail', args=['00000000-0000-0000-0000-000000000000']))
        self.assertContains(response, 'not found', status_code=404)
