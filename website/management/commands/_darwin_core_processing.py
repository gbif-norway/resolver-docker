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
    columns = get_columns(file_obj.readline())
    with connection.cursor() as cursor:
        create_temp_table(cursor, columns)
        insert_file(cursor, file_obj)
        create_id_column(cursor, id_column)
        drop_invalid_uuids(cursor)
        insert_json_into_replacement_table(cursor, columns)
        cursor.execute("SELECT COUNT(*) FROM temp")
        count = cursor.fetchone()
        cursor.execute('DROP TABLE temp')
    return count[0]

def get_columns(first_line):
    return first_line.rstrip().lower().split('\t')

def create_temp_table(cursor, columns):
    sql = 'CREATE TABLE temp ("' + '" text, "'.join(columns) + '" text);'
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
    drop_invalid_uuids = "DELETE FROM temp WHERE id !~ '([urnURN]+:[a-zA-Z]+:)?[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$'"
    cursor.execute(drop_invalid_uuids)

def insert_json_into_replacement_table(cursor, columns):
    json_columns = ["'%s', %s" % (key, key) for key in columns]
    make_json_sql = "SELECT uuid(id) AS uuid, json_build_object(" + ', '.join(json_columns) + ") AS data FROM temp;"
    insert_sql = "INSERT INTO replacement_table(uuid, data) " + make_json_sql
    cursor.execute(insert_sql)

def _email_message(subject, message):
    mail.mail_admins(subject, message, fail_silently=True)

