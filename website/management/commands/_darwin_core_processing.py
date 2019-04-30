import sys
from django.core import mail
from django.db import connection

def get_core_id(core_type):
     CORE_ID_MAPPINGS = {'event': 'eventid', 'occurrence': 'occurrenceid', 'extendedmeasurementorfact': 'measurementid', 'measurementorfact': 'measurementid', 'taxon': 'taxonid'}
     try:
         return CORE_ID_MAPPINGS[core_type]
     except KeyError as e:
         return False

def copy_csv_to_replacement_table(file_obj, id_column):
    with connection.cursor() as cursor:
        create_temp_table(cursor, get_columns(file_obj.readline()))
        print('created temp table')
        insert_file(cursor, file_obj)
        print('inserted file')
        create_id_column(cursor, id_column)
        print('created id col')
        drop_invalid_uuids(cursor)
        print('dropped invalid ids')
        insert_json_into_replacement_table(cursor)
        print('inserted into replacement table')
        cursor.execute("SELECT COUNT(*) FROM temp")
        count = cursor.fetchone()
        cursor.execute('DROP TABLE temp')
        print(count[0])
    return count[0]

def get_columns(first_line):
    return first_line.decode("utf-8").rstrip().lower().split('\t')

def create_temp_table(cursor, columns):
    sql = 'DROP TABLE IF EXISTS temp; CREATE TABLE temp ("' + '" text, "'.join(columns) + '" text);'
    cursor.execute(sql)

def insert_file(cursor, file_obj):
    copy_sql = "COPY temp FROM stdin DELIMITER AS '\t'"
    cursor.copy_expert(sql=copy_sql, file=file_obj)

def create_id_column(cursor, id_column):
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='temp' and column_name='id';")
    if cursor.fetchone() == ('id',):
        return
    cursor.execute("ALTER TABLE temp ADD COLUMN id text")
    cursor.execute("UPDATE temp SET id = %s" % (id_column))

def drop_invalid_uuids(cursor):
    drop_invalid_uuids = "DELETE FROM temp WHERE id !~ '^([urnURN]+:[a-zA-Z]+:)?[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$'"
    cursor.execute(drop_invalid_uuids)
    # TODO put uuid prefix in separate col? https://stackoverflow.com/questions/49381318/python-uuid-handle-urn-with-namespace
    remove_uuid_prefix = "UPDATE temp SET id = REPLACE(id, 'urn:uuid:', '')"
    cursor.execute(remove_uuid_prefix)

def insert_json_into_replacement_table(cursor):
    make_json_sql = "SELECT uuid(id) AS uuid, row_to_json(temp) AS data FROM temp;"
    insert_sql = "INSERT INTO replacement_table(uuid, data) " + make_json_sql
    cursor.execute(insert_sql)

def _email_message(subject, message):
    mail.mail_admins(subject, message, fail_silently=True)

