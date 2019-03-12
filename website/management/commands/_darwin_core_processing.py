import sys
from django.core import mail
from website.models import DarwinCoreObject
import csv

 def get_core_id(core_type):
     CORE_ID_MAPPINGS = {'event': 'eventID', 'occurrence': 'occurrenceID', 'extendedmeasurementorfact': 'measurementID', 'measurementorfact': 'measurementID', 'taxon': 'taxonID'}
     try:
         return CORE_ID_MAPPINGS[core_type]
     except KeyError as e:
         return False

def build_darwin_core_objects(core_id_key, file_obj):
    darwin_core_objects = []
    with csv.DictReader(file_obj, delimiter="\t") as reader:
        for row in reader:  # TODO There should always be a row[core_id_key], but perhaps should add error handling just in case
            darwin_core_objects.append(DarwinCoreObject(id=row[core_id_key], data=row))
    return darwin_core_objects

