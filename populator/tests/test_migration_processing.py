from populator.management.commands import _migration_processing as migration_processing
from populator.management.commands.populate_resolver import create_duplicates_file
from populator.models import ResolvableObjectMigration
from django.db import connection, transaction
from django.test import TestCase, TransactionTestCase
from django.forms.models import model_to_dict
import os


class MigrationProcessingTest(TestCase):
    def _get_temp_count(self):
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM temp")
            return cursor.fetchone()[0]

    def test_import_dwca_imports_rows(self):
        migration_processing.import_dwca('my_dataset_id', '/code/populator/tests/mock_data/dwca-seabird_estimates-v1.0.zip')
        self.assertEqual(ResolvableObjectMigration.objects.count(), 20191)

    def test_import_dwca_skips_bad_rows(self):
        migration_processing.import_dwca('my_dataset_id', '/code/populator/tests/mock_data/dwc_archive_bad_rows.zip')
        self.assertEqual(ResolvableObjectMigration.objects.count(), 11)

    def test_get_core_id(self):
        self.assertEqual('measurementid', migration_processing.get_core_id('measurementorfact'))
        self.assertEqual('occurrenceid', migration_processing.get_core_id('occurrence'))

    def test_get_core_id_fail(self):
        self.assertEqual(False, migration_processing.get_core_id('measurement'))

    def test_get_columns(self):
        heading_string = b'HEADING1,\tHeading,heading\theading3\theading4\t'
        headings_result = ['heading1,', 'heading,heading', 'heading3', 'heading4']
        self.assertEqual(headings_result, migration_processing.get_columns(heading_string))

    def test_create_temp_table(self):
        headings = ['heading1', 'heading2', 'order', 'heading4']
        with connection.cursor() as cursor:
            migration_processing.create_temp_table(headings)
            cursor.execute('SELECT * FROM temp')
            results = cursor.fetchall()
            self.assertEqual(len(results), 0)
            self.assertEqual(headings, [col[0] for col in cursor.description])

    def test_create_temp_table_with_previously_existing_table(self):
        headings = ['heading1', 'heading2', 'order', 'heading4']
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (test text)")
            migration_processing.create_temp_table(headings)
            cursor.execute('SELECT * FROM temp')
            cursor.fetchall()
            self.assertEqual(headings, [col[0] for col in cursor.description])

    def test_sync_id_column_add_new_id_col(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (eventid text, heading2 text, heading3 text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            self.assertTrue(migration_processing.sync_id_column('eventid'))
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'eventid': 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'id': 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'heading2': 'a', 'heading3': 'b'})

    def test_sync_id_column_replace_id_col(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text,  eventid text, heading text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'urn:uuid:2', 'urn:uuid:1', 'b')")
            self.assertTrue(migration_processing.sync_id_column('occurrenceid'))
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': 'urn:uuid:2','occurrenceid':'urn:uuid:2', 'eventid': 'urn:uuid:1', 'heading': 'b'})

    def test_sync_occurrence_id_column_with_event_core(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text,  heading2 text, heading3 text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'urn:uuid:2', 'a', 'b')")
            self.assertTrue(migration_processing.sync_id_column('occurrenceid'))
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': 'urn:uuid:2','occurrenceid':'urn:uuid:2', 'heading2': 'a', 'heading3': 'b'})

    def test_sync_id_column_with_no_coreid_col_returns_false(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, heading text)")  # This happens e.g. http://data.nina.no:8080/ipt/archive.do?r=arko_strandeng occurrence.txt
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'b')")
            self.assertFalse(migration_processing.sync_id_column('occurrenceid'))

    def test_add_dataset_id(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, occurrenceid text, "order" text, heading3 text)')
            cursor.execute("INSERT INTO temp VALUES ('a', 'b', 'c', 'd')")
            cursor.execute("INSERT INTO temp VALUES ('e', 'f', 'g', 'h')")
            migration_processing.add_dataset_id('2b52369a-7fe0-4d28-b88c-c882c0ce71d8')
            cursor.execute("SELECT * FROM temp")
            results = [('a', 'b', 'c', 'd', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8'), ('e', 'f', 'g', 'h', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8')]
            self.assertEqual(cursor.fetchall(), results)

    def test_add_dataset_id_with_preexisting_dataset_id(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, occurrenceid text, "order" text, datasetid text)')
            cursor.execute("INSERT INTO temp VALUES ('a', 'b', 'c', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8')")
            cursor.execute("INSERT INTO temp VALUES ('e', 'f', 'g', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8')")
            migration_processing.add_dataset_id('2b52369a-7fe0-4d28-b88c-c882c0ce71d8')
            cursor.execute("SELECT * FROM temp")
            results = [('a', 'b', 'c', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8'), ('e', 'f', 'g', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8')]
            self.assertEqual(cursor.fetchall(), results)

    def test_insert_json_into_migration_table(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, occurrenceid text, "order" text, heading3 text)')
            cursor.execute("INSERT INTO temp VALUES ('ba128c35-5e8f-408f-8597-00b1972dace1', 'ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
        migration_processing.insert_json_into_migration_table('dataset_id', 'occurrence')
        expected = {'id': 'ba128c35-5e8f-408f-8597-00b1972dace1', 'type': 'occurrence',
                    'data': {'id': 'ba128c35-5e8f-408f-8597-00b1972dace1',
                             'occurrenceid': 'ba128c35-5e8f-408f-8597-00b1972dace1',
                             'order': 'a', 'heading3': 'b'},
                    'dataset_id': 'dataset_id'}
        self.assertEqual([model_to_dict(x) for x in ResolvableObjectMigration.objects.all()], [expected])

    def test_insert_json_into_migration_table_multiple(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, scientificname text)')
            cursor.execute("INSERT INTO temp VALUES ('a', 'eudyptes')")
            cursor.execute("INSERT INTO temp VALUES ('b', 'another')")
        migration_processing.insert_json_into_migration_table('dataset_id', 'occurrence')
        expected = [{'id': 'a', 'data': {'id': 'a', 'scientificname': 'eudyptes'}, 'type': 'occurrence', 'dataset_id': 'dataset_id'},
                    {'id': 'b', 'data': {'id': 'b', 'scientificname': 'another'}, 'type': 'occurrence', 'dataset_id': 'dataset_id'}]
        self.assertEqual([model_to_dict(x) for x in ResolvableObjectMigration.objects.all()], expected)

    def test_insert_with_previous_dataset(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('a', 'a-name')")
            cursor.execute("INSERT INTO temp VALUES ('b', 'b-name')")
        migration_processing.insert_json_into_migration_table('a_d_id', 'occurrence')

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE temp')
            cursor.execute('CREATE TABLE temp (id text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('c', 'c-name')")
            cursor.execute("INSERT INTO temp VALUES ('d', 'd-name')")
        migration_processing.insert_json_into_migration_table('b_d_id', 'occurrence')
        expected = [
            {'id': 'a', 'data': {'id': 'a', 'sname': 'a-name'}, 'type': 'occurrence', 'dataset_id': 'a_d_id'},
            {'id': 'b', 'data': {'id': 'b', 'sname': 'b-name'}, 'type': 'occurrence', 'dataset_id': 'a_d_id'},
            {'id': 'c', 'data': {'id': 'c', 'sname': 'c-name'}, 'type': 'occurrence', 'dataset_id': 'b_d_id'},
            {'id': 'd', 'data': {'id': 'd', 'sname': 'd-name'}, 'type': 'occurrence', 'dataset_id': 'b_d_id'},
        ]
        self.assertEqual([model_to_dict(x) for x in ResolvableObjectMigration.objects.all()], expected)

    def test_insert_with_previous_dataset_with_duplicates_keeps_first_result(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('x', 'a-name')")
            cursor.execute("INSERT INTO temp VALUES ('b', 'b-name')")
        migration_processing.insert_json_into_migration_table('a_d_id', 'occurrence')

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE temp')
            cursor.execute('CREATE TABLE temp (id text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('x', 'c-name')")
            cursor.execute("INSERT INTO temp VALUES ('d', 'd-name')")
        migration_processing.insert_json_into_migration_table('b_d_id', 'occurrence')
        expected = [
            {'id': 'x', 'data': {'id': 'x', 'sname': 'a-name'}, 'type': 'occurrence', 'dataset_id': 'a_d_id'},
            {'id': 'b', 'data': {'id': 'b', 'sname': 'b-name'}, 'type': 'occurrence', 'dataset_id': 'a_d_id'},
            {'id': 'd', 'data': {'id': 'd', 'sname': 'd-name'}, 'type': 'occurrence', 'dataset_id': 'b_d_id'},
        ]
        self.assertEqual([model_to_dict(x) for x in ResolvableObjectMigration.objects.all()], expected)

    def test_get_duplicates_works_with_records_with_duplicate_ids(self):
        file = '/code/test_duplicates.txt'
        create_duplicates_file(file)
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('x', 'a-name')")
            cursor.execute("INSERT INTO temp VALUES ('b', 'b-name')")
        migration_processing.insert_json_into_migration_table('a_d_id', 'occurrence')

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE temp')
            cursor.execute('CREATE TABLE temp (id text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('x', 'c-name')")
            cursor.execute("INSERT INTO temp VALUES ('d', 'd-name')")
        migration_processing.get_duplicates('b_d_id', 'occurrence', file)

        with open(file) as f:
            content = f.readlines()

        self.assertEqual(len(content), 2)  # Including header
        result = [line.rstrip('\n') for line in content]
        expected = ['x', '{"id":"x","sname":"c-name"}', 'b_d_id', 'occurrence', '{"id": "x", "sname": "a-name"}', 'a_d_id']
        self.assertEqual(result[1].split('|'), expected)

    def test_get_duplicates_works_with_weird_char_encoding(self):
        file = '/code/duplicates.txt'
        create_duplicates_file(file)
        count = migration_processing.import_dwca('my_dataset_id', '/code/populator/tests/mock_data/dwca-molltax-v1.195.zip')
        self.assertEqual(count, 23227)
        count = migration_processing.import_dwca('my_dataset_id', '/code/populator/tests/mock_data/dwca-molltax-v1.195.zip')
        self.assertEqual(count, 0)
        self.assertEqual(ResolvableObjectMigration.objects.count(), 23227)

        with open(file) as f:
            content = f.readlines()
        self.assertEqual(len(content), 23228)
        os.remove(file)

    def test_occurrence_records_get_occurrence_id(self):  # Necessary for event-based datasets
        pass
