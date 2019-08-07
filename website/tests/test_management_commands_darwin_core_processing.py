from website.management.commands import _darwin_core_processing
from django.test import TestCase
from django.db import connection
from io import StringIO
import gzip
from zipfile import ZipFile
import uuid

class DarwinCoreProcessingTest(TestCase):
    def test_get_core_id(self):
        self.assertEqual('measurementid', _darwin_core_processing.get_core_id('measurementorfact'))
        self.assertEqual('occurrenceid', _darwin_core_processing.get_core_id('occurrence'))

    def test_get_core_id_fail(self):
        self.assertEqual(False, _darwin_core_processing.get_core_id('measurement'))

    def test_copy_csv_to_replacement_table_small(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE replacement_table (uuid uuid, data jsonb)")
        with ZipFile('website/tests/occurrence_test_file_small.txt.zip', 'r') as file_obj:
            count = _darwin_core_processing.copy_csv_to_replacement_table(file_obj.open('occurrence.txt'), 'occurrenceid')
        self.assertEqual(count, 5000)

    def test_copy_csv_to_replacement_table_large(self):
        return
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE replacement_table (uuid uuid, data jsonb)")
        with gzip.open('website/tests/occurrence_test_file_large.txt.gz', 'rt') as file_obj:
            count = _darwin_core_processing.copy_csv_to_replacement_table(file_obj, 'occurrenceid')
        self.assertEqual(count, 1700000)

    def test_copy_csv_to_replacement_table_with_no_id_adds_no_records(self):
        with ZipFile('website/tests/occurrence_test_file_small.txt.zip', 'r') as file_obj:
            count = _darwin_core_processing.copy_csv_to_replacement_table(file_obj.open('occurrence.txt'), 'eventid')
        self.assertEqual(count, 0)

    def test_get_columns(self):
        heading_string = b'HEADING1,\tHeading,heading\theading3\theading4\t'
        headings_result = ['heading1,', 'heading,heading', 'heading3', 'heading4']
        self.assertEqual(headings_result, _darwin_core_processing.get_columns(heading_string))

    def test_create_temp_table(self):
        with connection.cursor() as cursor:
            headings = ['heading1', 'heading2', 'order', 'heading4']
            _darwin_core_processing.create_temp_table(cursor, headings)
            temp = cursor.execute('SELECT * FROM temp')
            results = cursor.fetchall()
            self.assertEqual(len(results), 0)
            self.assertEqual(headings, [col[0] for col in cursor.description])

    def test_create_temp_table_with_previously_existing_table(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (test text)")
            headings = ['heading1', 'heading2', 'order', 'heading4']
            _darwin_core_processing.create_temp_table(cursor, headings)
            temp = cursor.execute('SELECT * FROM temp')
            results = cursor.fetchall()
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

    def test_sync_id_column_add_new_id_col(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (eventid text, heading2 text, heading3 text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            _darwin_core_processing.sync_id_column(cursor, 'eventid')
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'eventid': 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'id': 'urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'heading2': 'a', 'heading3': 'b'})

    def test_sync_id_column_replace_id_col(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, occurrenceid text,  heading2 text, heading3 text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:1', 'urn:uuid:2', 'a', 'b')")
            _darwin_core_processing.sync_id_column(cursor, 'occurrenceid')
            cursor.execute('SELECT * FROM temp')
            columns = [col[0] for col in cursor.description]
            self.assertEqual(dict(zip(columns, cursor.fetchone())), {'id': 'urn:uuid:2','occurrenceid':'urn:uuid:2', 'heading2': 'a', 'heading3': 'b'})

    def test_drop_invalid_uuids(self):
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE temp (id text, heading2 text, heading3 text)")
            cursor.execute("INSERT INTO temp VALUES ('urn:uuid:ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            cursor.execute("INSERT INTO temp VALUES ('abcd-ef-g', 'a', 'b')")
            cursor.execute("INSERT INTO temp VALUES ('aa128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            cursor.execute("INSERT INTO temp VALUES ('', 'a', 'b')")
            _darwin_core_processing.drop_invalid_uuids(cursor)
            cursor.execute("SELECT * FROM temp")
            self.assertEqual(cursor.fetchall(), [('ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b'), ('aa128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')])

    def test_insert_json_into_new_replacement_table(self):
        # The first temp table which gets migrated to the replacement table will have this use case
        with connection.cursor() as cursor:
            cursor.execute('CREATE TABLE temp (id text, occurrenceid text, "order" text, heading3 text)')
            cursor.execute("INSERT INTO temp VALUES ('ba128c35-5e8f-408f-8597-00b1972dace1', 'ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            cursor.execute("CREATE TABLE replacement_table (uuid UUID PRIMARY KEY, data JSONB)")
            _darwin_core_processing.insert_json_into_replacement_table(cursor)
            cursor.execute("SELECT * FROM replacement_table")
            results = [(uuid.UUID('ba128c35-5e8f-408f-8597-00b1972dace1'), {'id': 'ba128c35-5e8f-408f-8597-00b1972dace1', 'occurrenceid': 'ba128c35-5e8f-408f-8597-00b1972dace1', 'order': 'a', 'heading3': 'b'})]
            self.assertEqual(cursor.fetchall(), results)

    def test_insert_big_json_into_new_replacement_table(self):
        # A harder to read but more realistic version of test_insert_json_into_new_replacement_table
        with connection.cursor() as cursor:
            alphabet = list(map(chr, range(97, 123)))
            # Generate this: keys = alphabet + [letter + letter for letter in alphabet]
            # values = { columns[i]:value for i, value in enumerate(list(range(1, 53))) }
            # keys[0] = 'id'
            cursor.execute("create table temp (id text, b text, c text, d text, e text, f text, g text, h text, i text, j text, k text, l text, m text, n text, o text, p text, q text, r text, s text, t text, u text, v text, w text, x text, y text, z text, aa text, bb text, cc text, dd text, ee text, ff text, gg text, hh text, ii text, jj text, kk text, ll text, mm text, nn text, oo text, pp text, qq text, rr text, ss text, tt text, uu text, vv text, ww text, xx text, yy text, zz text)")
            cursor.execute("insert into temp values ('ba128c35-5e8f-408f-8597-00b1972dace1', 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52)")
            cursor.execute("CREATE TABLE replacement_table (uuid UUID PRIMARY KEY, data JSONB)")
            _darwin_core_processing.insert_json_into_replacement_table(cursor)
            cursor.execute("select * from replacement_table")
            results = [(uuid.UUID('ba128c35-5e8f-408f-8597-00b1972dace1'), {'id': 'ba128c35-5e8f-408f-8597-00b1972dace1' , 'b': '2', 'c': '3', 'd': '4', 'e': '5', 'f': '6', 'g': '7', 'h': '8', 'i': '9', 'j': '10', 'k': '11', 'l': '12', 'm': '13', 'n': '14', 'o': '15', 'p': '16', 'q': '17', 'r': '18', 's': '19', 't': '20', 'u': '21', 'v': '22', 'w': '23', 'x': '24', 'y': '25', 'z': '26', 'aa': '27', 'bb': '28', 'cc': '29', 'dd': '30', 'ee': '31', 'ff': '32', 'gg': '33', 'hh': '34', 'ii': '35', 'jj': '36', 'kk': '37', 'll': '38', 'mm': '39', 'nn': '40', 'oo': '41', 'pp': '42', 'qq': '43', 'rr': '44', 'ss': '45', 'tt': '46', 'uu': '47', 'vv': '48', 'ww': '49', 'xx': '50', 'yy': '51', 'zz': '52'})]
            self.assertEqual(cursor.fetchall(), results)

    def test_insert_json_into_pre_existing_replacement_table(self):
        # As different dwc files are looped through (from different endpoints, or different files at the same endpoint), new sets of records will get inserted into replacement_table
        with connection.cursor() as cursor:
            # Pre existing replacement table
            cursor.execute("CREATE TABLE replacement_table (uuid UUID PRIMARY KEY, data JSONB)")
            cursor.execute("""INSERT INTO replacement_table VALUES (UUID('f2f84497-b3bf-493a-bba9-7c68e6def80b'), '{"some_data": "some_value"}')""")
            cursor.execute("SELECT COUNT(*) FROM replacement_table")
            self.assertEqual(cursor.fetchone()[0], 1)

            # New temp table
            cursor.execute('CREATE TABLE temp (id text, occurrenceid text, "order" text, heading3 text)')
            cursor.execute("INSERT INTO temp VALUES ('ba128c35-5e8f-408f-8597-00b1972dace1', 'ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")

            # Test new values and existing values are in replacement table
            _darwin_core_processing.insert_json_into_replacement_table(cursor)
            cursor.execute("SELECT * FROM replacement_table")
            results = [(uuid.UUID('f2f84497-b3bf-493a-bba9-7c68e6def80b'), {'some_data': 'some_value'}),
                (uuid.UUID('ba128c35-5e8f-408f-8597-00b1972dace1'), {'occurrenceid': 'ba128c35-5e8f-408f-8597-00b1972dace1', 'id': 'ba128c35-5e8f-408f-8597-00b1972dace1', 'order': 'a', 'heading3': 'b'})]
            self.assertEqual(cursor.fetchall(), results)

    def test_insert_duplicate_ids_into_pre_existing_replacement_table(self):
        # Sometimes an occurrence_id will be included in two different datasets. For the moment, the resolver will just provide the information from the first dataset
        with connection.cursor() as cursor:
            # Pre existing replacement table
            cursor.execute("CREATE TABLE replacement_table (uuid UUID PRIMARY KEY, data JSONB)")
            cursor.execute("""INSERT INTO replacement_table VALUES (UUID('f2f84497-b3bf-493a-bba9-7c68e6def80b'), '{"some_data": "some_value"}')""")

            # New temp table with duplicate values
            cursor.execute('CREATE TABLE temp (id text, occurrenceid text, "order" text, heading3 text)')
            cursor.execute("INSERT INTO temp VALUES ('ba128c35-5e8f-408f-8597-00b1972dace1', 'ba128c35-5e8f-408f-8597-00b1972dace1', 'a', 'b')")
            cursor.execute("INSERT INTO temp VALUES ('f2f84497-b3bf-493a-bba9-7c68e6def80b', 'ba128c35-5e8f-408f-8597-00b1972dace1', 'c', 'd')")

            # Test no duplicates are in replacement table
            _darwin_core_processing.insert_json_into_replacement_table(cursor)
            cursor.execute("SELECT * FROM replacement_table")
            results = [(uuid.UUID('f2f84497-b3bf-493a-bba9-7c68e6def80b'), {'some_data': 'some_value'}),
                (uuid.UUID('ba128c35-5e8f-408f-8597-00b1972dace1'), {'occurrenceid': 'ba128c35-5e8f-408f-8597-00b1972dace1', 'id': 'ba128c35-5e8f-408f-8597-00b1972dace1', 'order': 'a', 'heading3': 'b'})]
            self.assertEqual(cursor.fetchall(), results)

