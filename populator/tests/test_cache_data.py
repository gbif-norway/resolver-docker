from io import StringIO
import psycopg2 as p
from psycopg2.extras import RealDictCursor
import responses
from unittest import TestCase, mock
from zipfile import ZipFile
import cache_data
from datetime import date, timedelta
import json


class CacheDataTest(TestCase):
    test_data = {'scientific_name': 'Draba verna', 'plant_parts': 'leaves', 'use': 'veterinary'}
    old_table = 'website_darwincoreobject'
    new_table = 'replacement_table'
    uuid_a = 'f2f84497-b3bf-493a-bba9-7c68e6def80b'
    uuid_b = 'g2f84497-b3bf-493a-bba9-7c68e6def80c'

    def setUp(self):
        with open('tests/create_db.sql', 'r') as reader:
            create_sql = reader.read()
        with p.connect('') as conn:
            with conn.cursor() as cursor:
                cursor.execute('DROP TABLE IF EXISTS replacement_table, website_history, website_darwincoreobject, website_statistic')
                cursor.execute(create_sql)
                cursor.execute('CREATE TABLE replacement_table (LIKE website_darwincoreobject INCLUDING ALL)')

    def tearDown(self):
        with p.connect('') as conn:
            with conn.cursor() as cursor:
                cursor.execute('DROP TABLE IF EXISTS replacement_table, website_history, website_darwincoreobject, website_statistic')

    def _insert_data(self, table, uuid, data, date=date.today()):
        with p.connect('') as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO {}(id, data, created_date) VALUES ('{}', '{}', '{}')".format(table, uuid, json.dumps(data), date))

    def _get_dict_result(self, table = 'website_history'):
        with p.connect('', cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT * FROM {}'.format(table))
                return cursor.fetchall()

    def test_adds_changes_in_history_table(self):
        self._insert_data(self.old_table, self.uuid_a, {'no_change': 'same', 'updated': 'old original', 'deleted': 'old deleted'})
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'same', 'updated': 'new updated', 'created': 'new created'})
        cache_data.merge_in_new_data()
        expected = [{'id': 1, 'darwin_core_object_id': self.uuid_a, 'changed_data': {'updated': 'old original', 'deleted': 'old deleted', 'created': None}, 'changed_date': date.today()}]
        self.assertEqual(self._get_dict_result(), expected)

    def test_does_not_add_to_history_table_if_no_changes(self):
        self._insert_data(self.old_table, self.uuid_a, {'no_change': 'should not be included in history'})
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'should not be included in history'})
        cache_data.merge_in_new_data()
        self.assertEqual(self._get_dict_result(), [])

    def test_new_dwc_entry_does_not_add_to_history_table(self):
        self._insert_data(self.new_table, self.uuid_a, {'new_record_added_to_dataset': 'should not be included in history'})
        cache_data.merge_in_new_data()
        self.assertEqual(self._get_dict_result(), [])

    def test_creates_updated_changes_in_darwin_core_object_table(self):
        past_date = date.today() - timedelta(days=5)
        self._insert_data(self.old_table, self.uuid_a, {'no_change': 'same', 'updated': 'old original', 'deleted': 'old deleted'}, past_date )
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'same', 'updated': 'new updated', 'created': 'new created'}, date.today())
        cache_data.merge_in_new_data()
        expected = [{'id': self.uuid_a, 'data': {'no_change': 'same', 'updated': 'new updated', 'created': 'new created'}, 'deleted_date': None, 'created_date': past_date}]
        self.assertEqual(self._get_dict_result(self.old_table), expected)

    def test_creates_single_record_in_darwin_core_object_table(self):
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'same', 'updated': 'new updated', 'created': 'new created'}, date.today())
        cache_data.merge_in_new_data()
        expected = [{'id': self.uuid_a, 'data': {'no_change': 'same', 'updated': 'new updated', 'created': 'new created'}, 'deleted_date': None, 'created_date': date.today()}]
        self.assertEqual(self._get_dict_result(self.old_table), expected)

    def test_creates_multiple_records_in_darwin_core_object_table(self):
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'same'}, date.today())
        self._insert_data(self.new_table, self.uuid_b, {'no_change': 'same'}, date.today())
        cache_data.merge_in_new_data()
        expected = [{'id': self.uuid_a, 'data': {'no_change': 'same'}, 'deleted_date': None, 'created_date': date.today()},
                    {'id': self.uuid_b, 'data': {'no_change': 'same'}, 'deleted_date': None, 'created_date': date.today()}]
        self.assertEqual(self._get_dict_result(self.old_table), expected)

    def test_adds_deleted_datestamp_for_removed_records_in_darwin_core_object_table(self):
        past_date = date.today() - timedelta(days=5)
        self._insert_data(self.old_table, self.uuid_a, {'no_change': 'same'}, past_date)
        cache_data.merge_in_new_data()
        expected = [{'id': self.uuid_a, 'data': {'no_change': 'same'}, 'deleted_date': date.today(), 'created_date': past_date}]
        self.assertEqual(self._get_dict_result(self.old_table), expected)

    def test_does_not_ovewrite_preexisting_deleted_datestamps(self):
        past_date_d = date.today() - timedelta(days=4)
        with p.connect('') as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO {}(id, data, created_date, deleted_date) VALUES ('{}', '{}', '{}', '{}')".format(self.old_table, self.uuid_a, json.dumps({'none': 'none'}), date.today() - timedelta(days=5), past_date_d))
        cache_data.merge_in_new_data()
        result = self._get_dict_result(self.old_table)
        self.assertEqual(result[0]['deleted_date'], past_date_d)

    def test_reset_refreshes_gbif_data(self):
        self._insert_data(self.new_table, self.uuid_b, {'new_data': 'refreshed from gbif'}, date.today())
        self._insert_data(self.old_table, self.uuid_a, {'old_data': 'now deleted on gbif'}, date.today() - timedelta(days=4))
        cache_data.reset()
        expected = [{'id': self.uuid_b, 'data': {'new_data': 'refreshed from gbif'}, 'deleted_date': None, 'created_date': date.today()}]
        self.assertEqual(self._get_dict_result(self.old_table), expected)

    def test_makes_no_change_to_created_date_for_data_that_remains_the_same(self):
        self._insert_data(self.new_table, self.uuid_a, {'no_change': 'none'}, date.today())
        self._insert_data(self.old_table, self.uuid_a, {'no_change': 'none'}, date.today() - timedelta(days=1))
        cache_data.merge_in_new_data()
        expected = [{'id': self.uuid_a, 'data': {'no_change': 'none'}, 'deleted_date': None, 'created_date': date.today() - timedelta(days=1)}]
        self.assertEqual(self._get_dict_result(self.old_table), expected)

    def test_reset_does_not_store_history(self):
        self._insert_data(self.new_table, self.uuid_b, {'new_data': 'refreshed from gbif'}, date.today())
        self._insert_data(self.old_table, self.uuid_a, {'old_data': 'now deleted on gbif'}, date.today() - timedelta(days=4))
        cache_data.reset()
        self.assertEqual(self._get_dict_result(), [])


if __name__ == '__main__':
    unittest.main()
