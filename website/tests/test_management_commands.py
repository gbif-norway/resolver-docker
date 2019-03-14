from io import StringIO
from django.core.management import call_command
from django.test import TestCase
import responses
import requests

class PopulateResolverTest(TestCase):

    def test_command_output(self):
        out = StringIO()
        #call_command('populate_resolver', stdout=out)
        #self.assertIn('test', out.getvalue())


