from io import StringIO
import responses
from unittest import mock
from populator.management.commands import _gbif_api as gbif_api
from populator.management.commands import _cache_data as cache_data
from website.models import History, DarwinCoreObject
from django.db import connection
from django.test import TestCase
from zipfile import ZipFile
from datetime import date, timedelta
import json


class CacheDataTest(TestCase):
    test_data = {'scientific_name': 'Draba verna', 'plant_parts': 'leaves', 'use': 'veterinary'}
    old_table = 'website_darwincoreobject'
    new_table = 'replacement_table'
    uuid_a = 'f2f84497-b3bf-493a-bba9-7c68e6def80b'
    uuid_b = 'g2f84497-b3bf-493a-bba9-7c68e6def80c'

    def setUp(self):
        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS replacement_table')
            cursor.execute('CREATE TABLE replacement_table (LIKE website_darwincoreobject INCLUDING ALL)')

    def tearDown(self):
        with connection.cursor() as cursor:
             cursor.execute('DROP TABLE IF EXISTS replacement_table')

    def _insert_data(self, table, uuid, data, date=date.today()):
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO {}(id, data, created_date) VALUES ('{}', '{}', '{}')".format(table, uuid, json.dumps(data), date))

    def test_adds_changes_in_history_table(self):
        self._insert_data(self.old_table, self.uuid_a, {'no_change': 'same', 'updated': 'old original', 'deleted': 'old deleted'})
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'same', 'updated': 'new updated', 'created': 'new created'})
        cache_data.merge_in_new_data()
        expected = History(id=1, darwin_core_object_id=self.uuid_a, changed_data={'updated': 'old original', 'deleted': 'old deleted', 'created': None}, changed_date=date.today())
        self.assertEqual(list(History.objects.all()), [expected])

    def test_does_not_add_to_history_table_if_no_changes(self):
        self._insert_data(self.old_table, self.uuid_a, {'no_change': 'should not be included in history'})
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'should not be included in history'})
        cache_data.merge_in_new_data()
        self.assertEqual(list(History.objects.all()), [])

    def test_new_dwc_entry_does_not_add_to_history_table(self):
        self._insert_data(self.new_table, self.uuid_a, {'new_record_added_to_dataset': 'should not be included in history'})
        cache_data.merge_in_new_data()
        self.assertEqual(list(History.objects.all()), [])

    def test_creates_updated_changes_in_darwin_core_object_table(self):
        past_date = date.today() - timedelta(days=5)
        self._insert_data(self.old_table, self.uuid_a, {'no_change': 'same', 'updated': 'old original', 'deleted': 'old deleted'}, past_date )
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'same', 'updated': 'new updated', 'created': 'new created'}, date.today())
        cache_data.merge_in_new_data()
        expected = DarwinCoreObject(id=self.uuid_a, data={'no_change': 'same', 'updated': 'new updated', 'created': 'new created'}, created_date=past_date)
        self.assertEqual(list(DarwinCoreObject.objects.all()), [expected])

    def test_creates_single_record_in_darwin_core_object_table(self):
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'same', 'updated': 'new updated', 'created': 'new created'}, date.today())
        cache_data.merge_in_new_data()
        expected = DarwinCoreObject(id=self.uuid_a, data={'no_change': 'same', 'updated': 'new updated', 'created': 'new created'}, created_date=date.today())
        self.assertEqual(list(DarwinCoreObject.objects.all()), [expected])

    def test_creates_multiple_records_in_darwin_core_object_table(self):
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'same'}, date.today())
        self._insert_data(self.new_table, self.uuid_b, {'no_change': 'same'}, date.today())
        cache_data.merge_in_new_data()
        expected = [DarwinCoreObject(id=self.uuid_a, data={'no_change': 'same'}, created_date=date.today()),
                    DarwinCoreObject(id=self.uuid_b, data={'no_change': 'same'}, created_date=date.today())]
        self.assertEqual(list(DarwinCoreObject.objects.all()), expected)

    def test_adds_deleted_datestamp_for_removed_records_in_darwin_core_object_table(self):
        past_date = date.today() - timedelta(days=5)
        self._insert_data(self.old_table, self.uuid_a, {'no_change': 'same'}, past_date)
        cache_data.merge_in_new_data()
        expected = DarwinCoreObject(id=self.uuid_a, data={'no_change': 'same'}, deleted_date=date.today(), created_date=past_date)
        self.assertEqual(list(DarwinCoreObject.objects.all()), [expected])

    def test_does_not_ovewrite_preexisting_deleted_datestamps(self):
        past_date_d = date.today() - timedelta(days=4)
        DarwinCoreObject.objects.create(id='1', data={'none': 'none'}, created_date=date.today() - timedelta(days=5), deleted_date=past_date_d)
        cache_data.merge_in_new_data()
        self.assertEqual(DarwinCoreObject.objects.first().deleted_date, past_date_d)

    def test_reset_refreshes_gbif_data(self):
        self._insert_data(self.new_table, self.uuid_b, {'new_data': 'refreshed from gbif'}, date.today())
        self._insert_data(self.old_table, self.uuid_a, {'old_data': 'now deleted on gbif'}, date.today() - timedelta(days=4))
        cache_data.reset()
        expected = DarwinCoreObject(id=self.uuid_b, data={'new_data': 'refreshed from gbif'}, created_date=date.today())
        self.assertEqual(list(DarwinCoreObject.objects.all()), [expected])

    def test_makes_no_change_to_created_date_for_data_that_remains_the_same(self):
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'none'}, date.today())
        self._insert_data(self.old_table, self.uuid_a, {'no_change': 'none'}, date.today() - timedelta(days=1))
        cache_data.merge_in_new_data()
        expected = DarwinCoreObject(id=self.uuid_a, data={'no_change': 'none'}, created_date=date.today() - timedelta(days=1))
        self.assertEqual(list(DarwinCoreObject.objects.all()), [expected])

    def test_reset_does_not_store_history(self):
        self._insert_data(self.new_table, self.uuid_b, {'new_data': 'refreshed from gbif'}, date.today())
        self._insert_data(self.old_table, self.uuid_a, {'old_data': 'now deleted on gbif'}, date.today() - timedelta(days=4))
        cache_data.reset()
        self.assertEqual(list(History.objects.all()), [])


if __name__ == '__main__':
    unittest.main()
