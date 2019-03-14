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

def build_darwin_core_objects(core_id_key, file_obj):
    darwin_core_objects = []
    columns = file_obj.readline().rstrip().split('\t')
    for (i, line) in enumerate(file_obj):
        _add_darwin_core_object(darwin_core_objects, line, columns)
        if i % 10000 == 0:
            with transaction.atomic():
                DarwinCoreObject.objects.bulk_create(darwin_core_objects)
            darwin_core_objects = []
    with transaction.atomic():  # Save the last set of darwin_core_objects
        DarwinCoreObject.objects.bulk_create(darwin_core_objects)
    return DarwinCoreObject.objects.all().count()

def _add_darwin_core_object(darwin_core_objects, line, columns):
    values = line.rstrip().split('\t')
    if len(columns) < len(values):
       print('error')
    data_json = dict(zip_longest(columns, values))
    if UUID(data_json['id']):
        darwin_core_objects.append(DarwinCoreObject(uuid=data_json['id'], data=data_json))

