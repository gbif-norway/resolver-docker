from website.management.commands import _darwin_core_processing
from django.test import TestCase

class DarwinCoreProcessingTest(TestCase):
    def test_get_core_id(self):
        self.assertEqual('measurementid', _darwin_core_processing.get_core_id('measurementorfact'))

    def test_get_core_id_fail(self):
        self.assertEqual(False, _darwin_core_processing.get_core_id('measurement'))

    def test_build_darwin_core_objects(self):
        with open('website/tests/occurrence_test_file_large.txt') as file_obj:
            darwin_core_objects = _darwin_core_processing.build_darwin_core_objects('occurrenceid', file_obj)
        self.assertEqual(darwin_core_objects, 3403810)
