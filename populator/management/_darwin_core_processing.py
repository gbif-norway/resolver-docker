import sys
import psycopg2 as p
from datetime import date

def get_core_id(core_type):
     CORE_ID_MAPPINGS = {'event': 'eventid', 'occurrence': 'occurrenceid', 'extendedmeasurementorfact': 'measurementid', 'measurementorfact': 'measurementid', 'taxon': 'taxonid'}
     try:
         return CORE_ID_MAPPINGS[core_type]
     except KeyError as e:
         return False

def copy_csv_to_replacement_table(file_obj, id_column, dataset_id):
    columns = get_columns(file_obj.readline())
    if id_column not in columns:
        return 0

    with p.connect('') as conn:
        with conn.cursor() as cursor:
            create_temp_table(cursor, columns)

    try: # On a test run 3/309 files would not insert TODO find out why, maybe weird char formatting?
        with p.connect('') as conn:
            with conn.cursor() as cursor:
                insert_file(cursor, file_obj)
    except p.DataError as e:
        with p.connect('') as conn:
            with conn.cursor() as cursor:
                cursor.execute("SET CLIENT_ENCODING TO 'LATIN1';")
                insert_file(cursor, file_obj)
                import pdb; pdb.set_trace()
    except Exception as e:
        print(e)
        print('error')

        import pdb; pdb.set_trace()
        #return 0

    with p.connect('') as conn:
        with conn.cursor() as cursor:
            sync_id_column(cursor, id_column)

    with p.connect('') as conn:
        with conn.cursor() as cursor:
            add_dataset_id(cursor, dataset_id)

    try:
        with p.connect('') as conn:
            with conn.cursor() as cursor:
                insert_json_into_replacement_table(cursor)
    except Exception as e:
        import pdb; pdb.set_trace()

    try:
        with p.connect('') as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM temp")
                count = cursor.fetchone()
                print(count)

        with p.connect('') as conn:
            with conn.cursor() as cursor:
                cursor.execute('DROP TABLE temp')
    except Exception as e:
        import pdb; pdb.set_trace()

    return count[0]

def get_columns(first_line):
    return first_line.decode("utf-8").rstrip().lower().split('\t')

def create_temp_table(cursor, columns):
    sql = 'DROP TABLE IF EXISTS temp; CREATE TABLE temp ("' + '" text, "'.join(columns) + '" text);'
    cursor.execute(sql)

def insert_file(cursor, file_obj):
    copy_sql = "COPY temp FROM stdin DELIMITER AS '\t'"
    cursor.copy_expert(sql=copy_sql, file=file_obj)

def sync_id_column(cursor, id_column):
    cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='id';")
    if cursor.fetchone()[0] == 0:
        cursor.execute("ALTER TABLE temp ADD COLUMN id text")
    cursor.execute("UPDATE temp SET id = %s" % id_column)

def add_dataset_id(cursor, dataset_id):
    cursor.execute('ALTER TABLE temp ADD COLUMN datasetid text')
    cursor.execute("UPDATE temp SET datasetid = '%s'" % dataset_id)

def insert_json_into_replacement_table(cursor):
    make_json_sql = """SELECT temp.id, row_to_json(temp) AS data, CURRENT_DATE
                       FROM temp LEFT JOIN replacement_table
                         ON replacement_table.id = temp.id
                       WHERE replacement_table.id IS NULL;"""
    insert_sql = "INSERT INTO replacement_table(id, data, created_date) " + make_json_sql
    cursor.execute(insert_sql)

