from django.test import TestCase
from populator.models import Statistic
from website.models import ResolvableObject, Dataset


class ResolvableObjectModelTests(TestCase):
    def setUp(self):
        self.dataset = Dataset.objects.create(id='dataset_id', data={'title': 'My dataset'})
        for item in '12345':
            ResolvableObject.objects.create(id=item, data={'test': item}, dataset=self.dataset)

    def test_sets_total_count(self):
        Statistic.objects.set_total_count()
        self.assertEqual(Statistic.objects.get(name='total_count').value, 5)  # Including the dataset object

    def test_overwrites_total_count(self):
        Statistic.objects.set_total_count()
        self.assertEqual(Statistic.objects.get(name='total_count').value, 5)
        ResolvableObject.objects.create(id='6', data={'test': '6'}, dataset=self.dataset)
        Statistic.objects.set_total_count()
        self.assertEqual(Statistic.objects.get(name='total_count').value, 6)

    def test_gets_total_count(self):
        self.assertEqual(Statistic.objects.get_total_count(), 5)
