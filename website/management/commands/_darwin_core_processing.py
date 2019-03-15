import sys
from django.core import mail
from website.models import DarwinCoreObject
import csv
from uuid import UUID
from django.db import transaction
from itertools import zip_longest
from django.db import connection

def get_core_id(core_type):
     CORE_ID_MAPPINGS = {'event': 'eventid', 'occurrence': 'occurrenceid', 'extendedmeasurementorfact': 'measurementid', 'measurementorfact': 'measurementid', 'taxon': 'taxonid'}
     try:
         return CORE_ID_MAPPINGS[core_type]
     except KeyError as e:
         return False

def create_darwin_core_objects(core_id_key, file_obj):
    darwin_core_objects = []
    columns = file_obj.readline().rstrip().split('\t')
    start = DarwinCoreObject.objects.all().count()
    for (i, line) in enumerate(file_obj):
        _add_darwin_core_object(darwin_core_objects, line, columns)
        if i % 10000 == 0:
            with transaction.atomic():
                DarwinCoreObject.objects.bulk_create(darwin_core_objects)
            darwin_core_objects = []
    with transaction.atomic():  # Save the last set of darwin_core_objects
        DarwinCoreObject.objects.bulk_create(darwin_core_objects)
    return DarwinCoreObject.objects.all().count() - start

def _add_darwin_core_object(darwin_core_objects, line, columns):
    values = line.rstrip().split('\t')
    if len(columns) < len(values):
        return

    data_json = dict(zip_longest(columns, values))
    try:
        UUID(data_json['id'])  # Do we need to use core_id_key here?
    except:
        return

    dwc_obj = DarwinCoreObject(uuid=data_json['id'], data=data_json)
    darwin_core_objects.append(dwc_obj)

def _email_message(subject, message):
    mail.mail_admins(subject, message, fail_silently=True)

def copy_csv_to_database(file_obj):
    columns = file_obj.readline().rstrip().lower().split('\t')
    id_column = 'id'
    with connection.cursor() as cursor:

        import pdb; pdb.set_trace()
        conn.commit()

def create_table(cursor, columns):
    create_table_sql = 'CREATE TABLE temp ("' + '" text, "'.join(columns) + '" text);'
    cursor.execute(create_table_sql)

def insert_file(cursor, file_obj)
    copy_sql = "COPY temp FROM stdin DELIMITER AS '\t'"
    cursor.copy_expert(sql=copy_sql, file=file_obj)

def drop_invalid_uuids(cursor, id_column):
    drop_invalid_uuids = "DELETE FROM temp WHERE " + id_column + " !~ '[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$'"
    cursor.execute(drop_invalid_uuids)

def insert_json_into_resolver(cursor, columns):
    json_columns = ["'%s', %s" % (key, key) for key in columns]
    make_json_sql = "SELECT " + id_column + ", json_build_object(" + ', '.join(json_columns) + ") AS data FROM temp;"
    # Going to have to do something the next bit in 2 parts, first bit will be to do an inner? join where only values not in django table are got, then inserted
    # then select the inverse, and do an update for those... need to update only if different? or just update anyway? in which case why not just del everything and insert the lot
    # https://stackoverflow.com/questions/1109061/insert-on-duplicate-update-in-postgresql
