from django.test import TestCase
from website.models import ResolvableObject, Dataset
from django.db.utils import IntegrityError


class ResolvableObjectModelTests(TestCase):
    def test_adds_objects_with_unique_ids(self):
        dataset = Dataset.objects.create(id='dataset_id', data={'label': 'My dataset'})
        ResolvableObject.objects.create(id='5c0884ce-608c-4716-ba0e-cb389dca5580',
                                        data={'id': '5c0884ce-608c-4716-ba0e-cb389dca5580', 'type': 'occurrence'},
                                        dataset=dataset)
        ResolvableObject.objects.create(id='urn:catalog:O:A:3228',
                                        data={'id': 'urn:catalog:O:A:3228', 'type': 'occurrence'},
                                        dataset=dataset)
        self.assertIs(ResolvableObject.objects.count(), 2)

    def test_validation_triggers_for_duplicated_id(self):
        dataset = Dataset.objects.create(id='dataset_id', data={'label': 'My dataset'})
        args = {'dataset_id': dataset.id, 'id': '5c0884ce-608c-4716-ba0e-cb389dca5580', 'data': {'id': '5c0884ce-608c-4716-ba0e-cb389dca5580', 'type': 'occurrence'}}
        ResolvableObject.objects.create(**args)
        self.assertRaises(IntegrityError, ResolvableObject.objects.create, **args)

