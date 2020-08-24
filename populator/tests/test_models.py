from django.test import TestCase
from populator.models import Statistic


class ResolvableObjectModelTests(TestCase):
    def test_sets_total_count(self):
        Statistic.objects.set_total_count(90)
        self.assertEqual(Statistic.objects.get(name='total_count').value, 90)

    def test_gets_total_count(self):
        Statistic.objects.set_total_count(100)
        self.assertEqual(Statistic.objects.get_total_count(), 100)
