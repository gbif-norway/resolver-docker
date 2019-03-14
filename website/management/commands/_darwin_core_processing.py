import sys
from django.core import mail
from website.models import DarwinCoreObject
import csv
from uuid import UUID
from django.db import transaction
from itertools import zip_longest

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

