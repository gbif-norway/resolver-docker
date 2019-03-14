import sys
from django.core import mail
from website.models import DarwinCoreObject
import csv
from uuid import UUID
import pandas as pd
from django.db import transaction
from itertools import zip_longest

def get_core_id(core_type):
     CORE_ID_MAPPINGS = {'event': 'eventid', 'occurrence': 'occurrenceid', 'extendedmeasurementorfact': 'measurementid', 'measurementorfact': 'measurementid', 'taxon': 'taxonid'}
     try:
         return CORE_ID_MAPPINGS[core_type]
     except KeyError as e:
         return False

def build_darwin_core_objects_test(core_id_key, file_obj):
    darwin_core_objects = []
    columns = file_obj.readline().rstrip().split('\t')
    for (i, line) in enumerate(file_obj):
        values = line.rstrip().split('\t')
        if len(columns) < len(values):
            import pdb; pdb.set_trace()
        data_json = dict(zip_longest(columns, values))
        if UUID(data_json['id']):
            darwin_core_objects.append(DarwinCoreObject(uuid=data_json['id'], data=data_json))
        if i % 10000 == 0:
            # print(i)
            with transaction.atomic():
                DarwinCoreObject.objects.bulk_create(darwin_core_objects)
            del darwin_core_objects
            darwin_core_objects = []
    with transaction.atomic():
        DarwinCoreObject.objects.bulk_create(darwin_core_objects)
    del darwin_core_objects
    return DarwinCoreObject.objects.all().count()

def build_darwin_core_objects(core_id_key, file_obj):
    chunks = pd.read_csv(file_obj, sep='\t', encoding='utf-8', dtype=str, chunksize=10000, keep_default_na=False, error_bad_lines=False)
    counter = 0
    for chunk in chunks:
        counter += len(chunk)
        if counter < 1611100:
            continue
        chunk.columns = map(str.lower, chunk.columns)
        print(counter)
        valid_uuids = chunk.loc[chunk[core_id_key].apply(_filter_invalid_uuids)]
        darwin_core_objects = [DarwinCoreObject(**{'uuid': row[core_id_key], 'data': row}) for row in valid_uuids.to_dict('records')]
        with transaction.atomic():
            DarwinCoreObject.objects.bulk_create(darwin_core_objects)
    import pdb; pdb.set_trace()
    return DarwinCoreObject.objects.all().count()

def _filter_invalid_uuids(dwc_uuid):
    try:
        UUID(dwc_uuid)
        return True
    except ValueError:
        return False

