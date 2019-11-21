from django.test import TestCase
from website.models import DarwinCoreObject
from django.db.utils import IntegrityError

class DarwinCoreObjectModelTests(TestCase):

    def test_adds_objects_with_unique_ids(self):
        obj_1 = DarwinCoreObject.objects.create(id='5c0884ce-608c-4716-ba0e-cb389dca5580', data={'id': '5c0884ce-608c-4716-ba0e-cb389dca5580', 'type': 'occurrence'})
        obj_2 = DarwinCoreObject.objects.create(id='urn:catalog:O:A:3228', data={'id': 'urn:catalog:O:A:3228', 'type': 'occurrence'})
        self.assertIs(DarwinCoreObject.objects.count(), 2)

    def test_validation_triggers_for_duplicated_id(self):
        args = {'id':'5c0884ce-608c-4716-ba0e-cb389dca5580', 'data':{'id': '5c0884ce-608c-4716-ba0e-cb389dca5580', 'type': 'occurrence'}}
        DarwinCoreObject.objects.create(**args)
        self.assertRaises(IntegrityError, DarwinCoreObject.objects.create, **args)

