from django.test import TestCase
from .models import DarwinCoreObject
from django.db.utils import IntegrityError

class DarwinCoreObjectModelTests(TestCase):

    def test_adds_objects_with_unique_uuids(self):
        obj_1 = DarwinCoreObject.objects.create(uuid='5c0884ce-608c-4716-ba0e-cb389dca5580', data={'id': '5c0884ce-608c-4716-ba0e-cb389dca5580', 'type': 'occurrence'})
        obj_2 = DarwinCoreObject.objects.create(uuid='14014f4f-97f9-4552-a607-619eda34c4e3', data={'id': '14014f4f-97f9-4552-a607-619eda34c4e3', 'type': 'occurrence'})
        self.assertIs(DarwinCoreObject.objects.count(), 2)

    def test_validation_triggers_for_duplicated_uuid(self):
        args = {'uuid':'5c0884ce-608c-4716-ba0e-cb389dca5580', 'data':{'id': '5c0884ce-608c-4716-ba0e-cb389dca5580', 'type': 'occurrence'}}
        DarwinCoreObject.objects.create(**args)
        self.assertRaises(IntegrityError, DarwinCoreObject.objects.create, **args)

