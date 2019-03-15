from website.management.commands import _darwin_core_processing
from django.test import TestCase
from django.db import connection

class DarwinCoreProcessingTest(TestCase):
    def test_get_core_id(self):
        self.assertEqual('measurementid', _darwin_core_processing.get_core_id('measurementorfact'))

    def test_get_core_id_fail(self):
        self.assertEqual(False, _darwin_core_processing.get_core_id('measurement'))

    def test_create_large_darwin_core_objects(self):
        return
        with open('website/tests/occurrence_test_file_large.txt') as file_obj:
            darwin_core_objects = _darwin_core_processing.create_darwin_core_objects('occurrenceid', file_obj)
        self.assertEqual(darwin_core_objects, 3403810)

    def test_build_invalid_darwin_core_objects(self):
        # Tests that rows with invalid uuids, blank uuids and unescaped tabs do not get added to db
        with open('website/tests/occurrence_test_file_complicated.txt') as file_obj:
            darwin_core_objects = _darwin_core_processing.create_darwin_core_objects('occurrenceid', file_obj)
        self.assertEqual(darwin_core_objects, 13)

    def test_database_copy(self):
        #with open('website/tests/occurrence_test_file_large.txt') as file_obj:
        with open('website/tests/occurrence_test_file_large.txt') as file_obj:
            darwin_core_objects = _darwin_core_processing.copy_csv_to_database(file_obj)

        with connection.cursor() as cursor:
            cursor.execute("select count(*) from temp")
            self.assertEqual(cursor.fetchone(), 13)



