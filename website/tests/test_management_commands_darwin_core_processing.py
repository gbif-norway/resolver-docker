from website.management.commands import _darwin_core_processing
from django.test import TestCase
from django.db import connection
from io import StringIO
import gzip

class DarwinCoreProcessingTest(TestCase):
    def test_get_core_id(self):
        self.assertEqual('measurementid', _darwin_core_processing.get_core_id('measurementorfact'))

    def test_get_core_id_fail(self):
        self.assertEqual(False, _darwin_core_processing.get_core_id('measurement'))

    def test_copy_csv_to_replacement_table_small(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE replacement_table (uuid uuid, data jsonb)")
        with open('website/tests/occurrence_test_file_small.txt') as file_obj:
            count = _darwin_core_processing.copy_csv_to_replacement_table(file_obj, 'occurrenceid')
        self.assertEqual(count, 5000)

    def test_copy_csv_to_replacement_table_large(self):
        return
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE replacement_table (uuid uuid, data jsonb)")
        with gzip.open('website/tests/occurrence_test_file_large.txt.gz', 'rt') as file_obj:
            count = _darwin_core_processing.copy_csv_to_replacement_table(file_obj, 'occurrenceid')
        self.assertEqual(count, 1700000)

    def test_get_columns(self):
        heading_string = "HEADING1,\tHeading,heading\theading3\theading4\t"
        headings_result = ['heading1,', 'heading,heading', 'heading3', 'heading4']
        self.assertEqual(headings_result, _darwin_core_processing.get_columns(heading_string))

    def test_create_temp_table(self):
        with connection.cursor() as cursor:
            headings = ['heading1', 'heading2', 'heading3', 'heading4']
            _darwin_core_processing.create_temp_table(cursor, headings)
            temp = cursor.execute('SELECT * FROM temp')
            results = cursor.fetchall()
            self.assertEqual(len(results), 0)
            self.assertEqual(headings, [col[0] for col in cursor.description])

    def test_insert_file(self):
        mock_file_content = [('abc', 'def', 'hij'), ('klm', 'nop', 'qrs'), ('tuv', 'wxy', 'z')]
        mock_file_string = 'abc\tdef\thij\nklm\tnop\tqrs\ntuv\twxy\tz'  # '\n'.join(['\t'.join(row) for row in mock_file_content])
        mock_file_string = StringIO(mock_file_string)
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, heading2 text, heading3 text)")
            _darwin_core_processing.insert_file(cursor, mock_file_string)
            cursor.execute("SELECT * FROM temp")
            self.assertEqual(cursor.fetchall(), mock_file_content)

    def test_create_id_column_add_id(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (occurrenceid text, heading2 text, heading3 text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            _darwin_core_processing.create_id_column(cursor, 'occurrenceid')
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'occurrenceid': 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'id': 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'heading2': 'a', 'heading3': 'b'})

    def test_create_id_column_add_id(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, heading2 text, heading3 text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            _darwin_core_processing.create_id_column(cursor, 'heading2')
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'heading2': 'a', 'heading3': 'b'})

    def test_drop_invalid_uuids(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, heading2 text, heading3 text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            cursor.execute("INSERT INTO temp VALUES ('abcd-ef-g', 'a', 'b')")
            cursor.execute("INSERT INTO temp VALUES ('aa128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            cursor.execute("INSERT INTO temp VALUES ('', 'a', 'b')")
            _darwin_core_processing.drop_invalid_uuids(cursor)
            cursor.execute("SELECT * FROM temp")
            self.assertEqual(cursor.fetchall(), [('urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b'), ('aa128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')])

    def test_insert_json_into_replacement_table(self):
        with connection.cursor() as cursor:
            columns = ['occurrenceid', 'heading2', 'heading3']
            cursor.execute("CREATE TABLE replacement_table (uuid uuid, data jsonb)")
            cursor.execute("""INSERT INTO replacement_table VALUES (UUID('f2f84497-b3bf-493a-bba9-7c68e6def80b'), '{"some_data": "some_value"}')""")
            cursor.execute("SELECT COUNT(*) FROM replacement_table")
            self.assertEqual(cursor.fetchone()[0], 1)
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text, heading2 text, heading3 text)")
            cursor.execute("INSERT INTO temp VALUES ('ba128c35-5e8f-408f-8597-00b1972dace1', 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            _darwin_core_processing.insert_json_into_replacement_table(cursor, columns)
            cursor.execute("SELECT * FROM replacement_table")
            import uuid
            results = [(uuid.UUID('f2f84497-b3bf-493a-bba9-7c68e6def80b'), {'some_data': 'some_value'}),
                (uuid.UUID('ba128c35-5e8f-408f-8597-00b1972dace1'), {'occurrenceid': 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'heading2': 'a', 'heading3': 'b'})]
            self.assertEqual(cursor.fetchall(), results)

