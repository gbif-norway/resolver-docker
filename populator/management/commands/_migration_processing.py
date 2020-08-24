from django.db import connection
import psycopg2 as p
import os
import json


def get_core_id(core_type):
     CORE_ID_MAPPINGS = {'event': 'eventid', 'occurrence': 'occurrenceid', 'extendedmeasurementorfact': 'measurementid', 'measurementorfact': 'measurementid', 'taxon': 'taxonid'}
     try:
         return CORE_ID_MAPPINGS[core_type]
     except KeyError as e:
         return False


def copy_csv_to_migration_table(file_obj, core_type, dataset_id):
    id_column = get_core_id(core_type)
    if not id_column:
        return 0
    columns = get_columns(file_obj.readline())
    if id_column not in columns:
        return 0
    create_temp_table(columns)

    try: # On a test run 3/309 files would not insert TODO find out why, maybe weird char formatting?
        with connection.cursor() as cursor:
            insert_file(file_obj)
    except p.errors.CharacterNotInRepertoire as e:
        with connection.cursor() as cursor:
            cursor.execute("SET CLIENT_ENCODING TO 'LATIN1';")
            insert_file(file_obj)
    except Exception as e:
        print(e)
        print('Cannot insert dwc file, possibly character error?')
        #return 0
        import pdb; pdb.set_trace()

    sync_id_column(id_column)
    #add_dataset_id(dataset_id)
    get_duplicates(dataset_id, core_type)
    insert_json_into_migration_table(dataset_id, core_type)

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM temp")
            count = cursor.fetchone()[0]
            cursor.execute('DROP TABLE temp')
        return count
    except Exception as e:
        print('cannot count temp exception')
        import pdb; pdb.set_trace()


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
        cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='temp' and column_name='datasetid';")
        if cursor.fetchone()[0] == 0:
            cursor.execute('ALTER TABLE temp ADD COLUMN datasetid text')
            cursor.execute("UPDATE temp SET datasetid = '%s'" % dataset_id)


def insert_json_into_migration_table(dataset_id, core_type):
    make_json_sql = """SELECT temp.id, row_to_json(temp), '{0}', '{1}'
                       FROM temp LEFT JOIN populator_resolvableobjectmigration AS j ON j.id = temp.id
                       WHERE j.id IS NULL;""".format(dataset_id, core_type)
    insert_sql = 'INSERT INTO populator_resolvableobjectmigration(id, data, dataset_id, type) ' + make_json_sql
    with connection.cursor() as cursor:
        cursor.execute(insert_sql)


def get_duplicates(dataset_id, core_type, file='/code/duplicates.txt'):
    with connection.cursor() as cursor:
        cursor.execute("""
        SELECT 
            temp.id AS id, 
            row_to_json(temp) AS new_data, 
            '{0}' AS new_datasetid, 
            '{1}' AS new_core_type, 
            j.data AS old_data, 
            j.dataset_id AS old_datasetid
        FROM temp 
        LEFT JOIN populator_resolvableobjectmigration AS j ON j.id = temp.id
        WHERE j.id IS NOT NULL;
        """.format(dataset_id, core_type))
        res = cursor.fetchall()

    with open(file, 'a') as f:
        for line in res:
            f.write('|'.join([json.dumps(x) for x in line]) + '\n')
