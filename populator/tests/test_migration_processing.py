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

    def test_import_dwca_works_for_non_core_files(self):
        migration_processing.import_dwca('my_dataset_id', '/code/populator/tests/mock_data/dwca_measurementorfact.zip')
        self.assertEqual(ResolvableObjectMigration.objects.filter(type='measurementorfact').count(), 10)
        self.assertEqual(ResolvableObjectMigration.objects.filter(type='occurrence').count(), 1)

    def test_import_dwca_links_mof_parents_to_occurrences(self):
        migration_processing.import_dwca('my_dataset_id', '/code/populator/tests/mock_data/dwca_measurementorfact.zip')
        mof = ResolvableObjectMigration.objects.filter(type='measurementorfact').first()
        self.assertEqual(mof.id, '2335276d-7d77-47be-8e33-91b6833b057b')  # Make sure it hasn't overwritten it with the core ID
        occ = ResolvableObjectMigration.objects.filter(type='occurrence').first()
        self.assertEqual(mof.parent, occ.id)  # This is the core ID, an occurrenceID in this case

    def test_import_dwca_links_mof_parents_to_events(self):
        migration_processing.import_dwca('my_dataset_id', '/code/populator/tests/mock_data/dwca_measurementorfact-events.zip')
        mof = ResolvableObjectMigration.objects.filter(type='measurementorfact').first()
        self.assertEqual(mof.id, '2335276d-7d77-47be-8e33-91b6833b057b')
        event = ResolvableObjectMigration.objects.filter(type='event').first()
        self.assertEqual(mof.parent, event.id)
        occ = ResolvableObjectMigration.objects.filter(type='occurrence').first()
        self.assertNotEqual(occ.id, occ.parent)
        self.assertEqual(occ.parent, event.id)

    def test_blank_fields_not_imported(self):
        migration_processing.import_dwca('my_dataset_id', '/code/populator/tests/mock_data/dwc_archive_bad_rows.zip')
        # The DwCA has a column 'sex' with no data, check that we don't import {"sex": ''} into the jsonb field
        first = ResolvableObjectMigration.objects.all().first()
        self.assertTrue('sex' not in first.data.keys())

    def test_get_id(self):
        self.assertEqual('measurementid', migration_processing.get_id('measurementorfact'))
        self.assertEqual('occurrenceid', migration_processing.get_id('occurrence'))

    def test_get_id_fail(self):
        self.assertEqual(False, migration_processing.get_id('measurement'))

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
            self.assertTrue(migration_processing.sync_id_column('eventid', 'eventid'))
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'eventid': 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'id': 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'heading2': 'a', 'heading3': 'b', 'parent': None})

    def test_sync_id_column_replace_id_col(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text,  eventid text, heading text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'urn:uuid:2', 'urn:uuid:1', 'b')")
            self.assertTrue(migration_processing.sync_id_column('occurrenceid', 'occurrenceid'))
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': 'urn:uuid:2', 'occurrenceid':'urn:uuid:2', 'eventid': 'urn:uuid:1', 'heading': 'b', 'parent': None})

    def test_sync_id_with_purl_materialsampleid_url(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text, materialsampleid text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'urn:uuid:1', 'http://purl.org/nhmuio/id/82b6903f-7613-4aba-b83b-948d0df6391a')")
            self.assertTrue(migration_processing.sync_id_column('occurrenceid', 'occurrenceid'))
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': '82b6903f-7613-4aba-b83b-948d0df6391a', 'occurrenceid': 'urn:uuid:1', 'materialsampleid': '82b6903f-7613-4aba-b83b-948d0df6391a', 'parent': None})

    def test_sync_id_with_purl_materialsampleid_uuid(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text, materialsampleid text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'urn:uuid:1', 'b55cbe46-5f2f-4c07-8223-9d4b0c8ed811')")
            self.assertTrue(migration_processing.sync_id_column('occurrenceid', 'occurrenceid'))
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': 'b55cbe46-5f2f-4c07-8223-9d4b0c8ed811', 'occurrenceid': 'urn:uuid:1', 'materialsampleid': 'b55cbe46-5f2f-4c07-8223-9d4b0c8ed811', 'parent': None})

    def test_sync_id_with_multiple_materialsampleid(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text, materialsampleid text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'urn:uuid:1', 'b55cbe46-5f2f-4c07-8223-9d4b0c8ed811|a55cbe46-5f2f-4c07-8223-9d4b0c8ed811')")
            self.assertTrue(migration_processing.sync_id_column('occurrenceid', 'occurrenceid'))
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            # Note that the second othercatalognumber gets deleted
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': 'b55cbe46-5f2f-4c07-8223-9d4b0c8ed811', 'occurrenceid': 'urn:uuid:1', 'materialsampleid': 'b55cbe46-5f2f-4c07-8223-9d4b0c8ed811', 'parent': None})

    def test_sync_id_with_duplicate_materialsampleid(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text, materialsampleid text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'urn:uuid:1', 'b55cbe46-5f2f-4c07-8223-9d4b0c8ed811')")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:2', 'urn:uuid:2', 'b55cbe46-5f2f-4c07-8223-9d4b0c8ed811')")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:3', 'urn:uuid:3', '3136D80A-E74C-11E4-A2DC-00155D012A60')")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:4', 'urn:uuid:4', '3136D80A-E74C-11E4-A2DC-00155D012A60,5FF9E4CE-E74D-11E4-891B-00155D012A60')")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:5', 'urn:uuid:5', 'http://purl.org/nhmuio/id/bdb4f713-5ef6-472b-9e9c-3d03dcb4b6b7')")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:6', 'urn:uuid:6', 'bdb4f713-5ef6-472b-9e9c-3d03dcb4b6b7')")
            self.assertTrue(migration_processing.sync_id_column('occurrenceid', 'occurrenceid'))
            cursor.execute('SELECT * FROM temp')
            self.assertEqual([('urn:uuid:1', 'urn:uuid:1', '', None), ('urn:uuid:2', 'urn:uuid:2', '', None),
                              ('urn:uuid:3', 'urn:uuid:3', '', None), ('urn:uuid:4', 'urn:uuid:4', '', None),
                              ('urn:uuid:5', 'urn:uuid:5', '', None), ('urn:uuid:6', 'urn:uuid:6', '', None)], cursor.fetchall())

    def test_sync_id_with_invalid_materialsampleid(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text, materialsampleid text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'urn:uuid:1', 'abc')")
            self.assertTrue(migration_processing.sync_id_column('occurrenceid', 'occurrenceid'))
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]

    def test_sync_id_ids_are_always_converted_to_lowercase(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text, materialsampleid text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1A', 'urn:uuid:1A', 'ABC')")
            migration_processing.sync_id_column('materialsampleid', 'occurrenceid')
            cursor.execute('SELECT * FROM temp')
            self.assertEqual([('abc', 'urn:uuid:1A', 'ABC', 'urn:uuid:1a')], cursor.fetchall())

    def test_purlfriendly_id_with_urn_prefix(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, parent text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', '')")
            migration_processing.purlfriendly_id_columns()
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': '1', 'parent': ''})

    def test_purlfriendly_id_with_url(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, parent text)")
            cursor.execute("INSERT INTO temp VALUES ('', 'http://purl.org/nhmuio/id/1')")
            migration_processing.purlfriendly_id_columns()
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': '', 'parent': '1'})

    def test_sync_occurrence_id_column_with_event_core(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text, heading2 text, heading3 text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'urn:uuid:2', 'a', 'b')")
            self.assertTrue(migration_processing.sync_id_column('occurrenceid', 'eventid'))  # Should keep eventid as parent
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': 'urn:uuid:2','occurrenceid':'urn:uuid:2', 'parent': 'urn:uuid:1', 'heading2': 'a', 'heading3': 'b'})

    def test_sync_id_column_with_no_coreid_col_returns_false(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, heading text)")  # This happens e.g. http://data.nina.no:8080/ipt/archive.do?r=arko_strandeng occurrence.txt
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'b')")
            self.assertFalse(migration_processing.sync_id_column('occurrenceid', 'occurrenceid'))

    def test_add_dataset_id(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, occurrenceid text, parent text, "order" text, heading3 text)')
            cursor.execute("INSERT INTO temp VALUES ('a', 'b', 'a', 'c', 'd')")
            cursor.execute("INSERT INTO temp VALUES ('e', 'f', 'e', 'g', 'h')")
            migration_processing.add_dataset_id('2b52369a-7fe0-4d28-b88c-c882c0ce71d8')
            cursor.execute("SELECT * FROM temp")
            results = [('a', 'b', 'a', 'c', 'd', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8'), ('e', 'f', 'e', 'g', 'h', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8')]
            self.assertEqual(cursor.fetchall(), results)

    def test_add_dataset_id_with_preexisting_dataset_id(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, occurrenceid text, parent text, "order" text, datasetid text)')
            cursor.execute("INSERT INTO temp VALUES ('a', 'b', 'a', 'c', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8')")
            cursor.execute("INSERT INTO temp VALUES ('e', 'f', 'e', 'g', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8')")
            migration_processing.add_dataset_id('2b52369a-7fe0-4d28-b88c-c882c0ce71d8')
            cursor.execute("SELECT * FROM temp")
            results = [('a', 'b', 'a', 'c', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8'), ('e', 'f', 'e', 'g', '2b52369a-7fe0-4d28-b88c-c882c0ce71d8')]
            self.assertEqual(cursor.fetchall(), results)

    def test_insert_json_into_migration_table(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, occurrenceid text, parent text, "order" text, heading3 text)')
            cursor.execute("INSERT INTO temp VALUES ('ba128c35-5e8f-408f-8597-00b1972dace1', 'ba128c35-5e8f-408f-8597-00b1972dace1', NULL, 'a', 'b')")
        migration_processing.insert_json_into_migration_table('dataset_id', 'occurrence')
        expected = {'id': 'ba128c35-5e8f-408f-8597-00b1972dace1', 'parent': None, 'type': 'occurrence',
                    'data': {'id': 'ba128c35-5e8f-408f-8597-00b1972dace1',
                             'occurrenceid': 'ba128c35-5e8f-408f-8597-00b1972dace1',
                             'order': 'a', 'heading3': 'b'},
                    'dataset_id': 'dataset_id'}
        self.assertEqual([model_to_dict(x) for x in ResolvableObjectMigration.objects.all()], [expected])

    def test_insert_json_into_migration_table_multiple(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, parent text, scientificname text)')
            cursor.execute("INSERT INTO temp VALUES ('a', NULL, 'eudyptes')")
            cursor.execute("INSERT INTO temp VALUES ('b', NULL, 'another')")
        migration_processing.insert_json_into_migration_table('dataset_id', 'occurrence')
        expected = [{'id': 'a', 'parent': None, 'data': {'id': 'a', 'scientificname': 'eudyptes'}, 'type': 'occurrence', 'dataset_id': 'dataset_id'},
                    {'id': 'b', 'parent': None, 'data': {'id': 'b', 'scientificname': 'another'}, 'type': 'occurrence', 'dataset_id': 'dataset_id'}]
        self.assertEqual([model_to_dict(x) for x in ResolvableObjectMigration.objects.all()], expected)

    def test_insert_json_into_migration_table_nulls(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, parent text, scientificname text)')
            cursor.execute("INSERT INTO temp VALUES ('a', NULL, NULL)")
            cursor.execute("INSERT INTO temp VALUES ('b', NULL, 'another')")
        migration_processing.insert_json_into_migration_table('dataset_id', 'occurrence')
        expected = [{'id': 'a', 'parent': None, 'data': {'id': 'a'}, 'type': 'occurrence', 'dataset_id': 'dataset_id'},
                    {'id': 'b', 'parent': None, 'data': {'id': 'b', 'scientificname': 'another'}, 'type': 'occurrence', 'dataset_id': 'dataset_id'}]
        self.assertEqual([model_to_dict(x) for x in ResolvableObjectMigration.objects.all()], expected)

    def test_insert_with_previous_dataset(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, parent text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('a', NULL, 'a-name')")
            cursor.execute("INSERT INTO temp VALUES ('b', NULL, 'b-name')")
        migration_processing.insert_json_into_migration_table('a_d_id', 'occurrence')

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE temp')
            cursor.execute('CREATE TABLE temp (id text, parent text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('c', NULL, 'c-name')")
            cursor.execute("INSERT INTO temp VALUES ('d', NULL, 'd-name')")
        migration_processing.insert_json_into_migration_table('b_d_id', 'occurrence')
        expected = [
            {'id': 'a', 'parent': None, 'data': {'id': 'a', 'sname': 'a-name'}, 'type': 'occurrence', 'dataset_id': 'a_d_id'},
            {'id': 'b', 'parent': None, 'data': {'id': 'b', 'sname': 'b-name'}, 'type': 'occurrence', 'dataset_id': 'a_d_id'},
            {'id': 'c', 'parent': None, 'data': {'id': 'c', 'sname': 'c-name'}, 'type': 'occurrence', 'dataset_id': 'b_d_id'},
            {'id': 'd', 'parent': None, 'data': {'id': 'd', 'sname': 'd-name'}, 'type': 'occurrence', 'dataset_id': 'b_d_id'},
        ]
        result = [model_to_dict(x) for x in ResolvableObjectMigration.objects.all()]
        self.assertEqual(sorted(result, key=lambda x: x['id']), expected)

    def test_remove_duplicates(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, parent text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('x', NULL, 'a-name')")
            cursor.execute("INSERT INTO temp VALUES ('b', NULL, 'b-name')")
        migration_processing.insert_json_into_migration_table('a_d_id', 'occurrence')

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE temp')
            cursor.execute('CREATE TABLE temp (id text, parent text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('x', NULL, 'c-name')")
            cursor.execute("INSERT INTO temp VALUES ('d', NULL, 'd-name')")
        migration_processing.remove_duplicates()
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM temp')
            temp_count = cursor.fetchone()[0]
        self.assertEqual(temp_count, 1)

    def test_insert_with_previous_dataset_with_duplicates_keeps_first_result(self):
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, parent text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('x', NULL, 'a-name')")
            cursor.execute("INSERT INTO temp VALUES ('b', NULL, 'b-name')")
        migration_processing.insert_json_into_migration_table('a_d_id', 'occurrence')

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE temp')
            cursor.execute('CREATE TABLE temp (id text, parent text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('x', NULL, 'c-name')")
            cursor.execute("INSERT INTO temp VALUES ('d', NULL, 'd-name')")
        migration_processing.remove_duplicates()
        migration_processing.insert_json_into_migration_table('b_d_id', 'occurrence')
        expected = [
            {'id': 'b', 'parent': None, 'data': {'id': 'b', 'sname': 'b-name'}, 'type': 'occurrence', 'dataset_id': 'a_d_id'},
            {'id': 'x', 'parent': None, 'data': {'id': 'x', 'sname': 'a-name'}, 'type': 'occurrence', 'dataset_id': 'a_d_id'},
            {'id': 'd', 'parent': None, 'data': {'id': 'd', 'sname': 'd-name'}, 'type': 'occurrence', 'dataset_id': 'b_d_id'},
        ]
        results = [model_to_dict(x) for x in ResolvableObjectMigration.objects.all()]
        self.assertEqual(results, expected)

    def test_record_duplicates_works_with_records_with_duplicate_ids(self):
        file = '/code/test_duplicates.txt'
        create_duplicates_file(file)
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, parent text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('x', NULL, 'a-name')")
            cursor.execute("INSERT INTO temp VALUES ('b', NULL, 'b-name')")
        migration_processing.insert_json_into_migration_table('a_d_id', 'occurrence')

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE temp')
            cursor.execute('CREATE TABLE temp (id text, parent text, sname text)')
            cursor.execute("INSERT INTO temp VALUES ('x', NULL, 'c-name')")
            cursor.execute("INSERT INTO temp VALUES ('d', NULL, 'd-name')")
        migration_processing.record_duplicates('b_d_id', 'occurrence', file)

        with open(file) as f:
            content = f.readlines()

        self.assertEqual(len(content), 2)  # Including header
        result = [line.rstrip('\n') for line in content]
        expected = ['x', '{"id":"x","parent":null,"sname":"c-name"}', 'b_d_id', 'occurrence', '{"id": "x", "sname": "a-name"}', 'a_d_id']
        self.assertEqual(result[1].split('|'), expected)

    def test_record_duplicates_works_with_weird_char_encoding(self):
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


class GetCoreTest(TestCase):
    def test_gets_event_core(self):
       result = migration_processing.get_core(['occurrence', 'taxon', 'measurementorfact', 'event'])
       self.assertEqual(result, 'event')

    def test_gets_occurrence_core(self):
        result = migration_processing.get_core(['simplemultimedia', 'occurrence', 'measurementorfact'])
        self.assertEqual(result, 'occurrence')

    def test_gets_taxon_core(self):
        result = migration_processing.get_core(['measurementorfact', 'taxon'])
        self.assertEqual(result, 'taxon')

    def test_gets_core_if_only_one_file(self):
        result = migration_processing.get_core(['occurrence'])
        self.assertEqual(result, 'occurrence')

    def test_returns_false_if_no_core(self):
        self.assertEqual(migration_processing.get_core([]), False)
