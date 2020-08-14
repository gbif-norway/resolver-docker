import sys
from django.db import connection
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

    create_temp_table(columns)

    try: # On a test run 3/309 files would not insert TODO find out why, maybe weird char formatting?
        with connection.cursor() as cursor:
            insert_file(file_obj)
    except p.DataError as e:
        with connection.cursor() as cursor:
            cursor.execute("SET CLIENT_ENCODING TO 'LATIN1';")
            insert_file(file_obj)
    except Exception as e:
        print(e)
        print('error')
        #return 0
        import pdb; pdb.set_trace()

    sync_id_column(id_column)
    add_dataset_id(dataset_id)
    insert_json_into_replacement_table()

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM temp")
            count = cursor.fetchone()[0]
            cursor.execute('DROP TABLE temp')
        return count
    except Exception as e:
        import pdb; pdb.set_trace()

    return 0

def get_columns(first_line):
    return first_line.decode("utf-8").rstrip().lower().split('\t')

def create_temp_table(columns):
    with connection.cursor() as cursor:
        sql = 'DROP TABLE IF EXISTS temp; CREATE TABLE temp ("' + '" text, "'.join(columns) + '" text);'
        cursor.execute(sql)

def insert_file(file_obj):
    with connection.cursor() as cursor:
        copy_sql = "COPY temp FROM stdin DELIMITER AS '\t'"
        cursor.copy_expert(sql=copy_sql, file=file_obj)

def sync_id_column(id_column):
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='id';")
        if cursor.fetchone()[0] == 0:
            cursor.execute("ALTER TABLE temp ADD COLUMN id text")
        cursor.execute("UPDATE temp SET id = %s" % id_column)

def add_dataset_id(dataset_id):
    with connection.cursor() as cursor:
        cursor.execute('ALTER TABLE temp ADD COLUMN datasetid text')
        cursor.execute("UPDATE temp SET datasetid = '%s'" % dataset_id)

def insert_json_into_replacement_table():
    make_json_sql = """SELECT temp.id, row_to_json(temp) AS data, CURRENT_DATE
                       FROM temp LEFT JOIN replacement_table
                         ON replacement_table.id = temp.id
                       WHERE replacement_table.id IS NULL;"""
    insert_sql = "INSERT INTO replacement_table(id, data, created_date) " + make_json_sql
    with connection.cursor() as cursor:
        cursor.execute(insert_sql)

