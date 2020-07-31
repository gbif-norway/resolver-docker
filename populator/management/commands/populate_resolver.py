from django.core.management.base import BaseCommand, CommandError
from django.core import mail
from website.models import Statistic, DarwinCoreObject
from populator.management.commands import _gbif_api, _darwin_core_processing, _cache_data
from django.db import connection
from datetime import date
import json

class Command(BaseCommand):
    help = 'Populates the resolver from datasets added to GBIF by Norwegian IPTs'

    def add_arguments(self, parser):
        parser.add_argument('--reset', action='store_true', help='Resets (clears cache) from GBIF')

    def handle(self, *args, **options):
        total_added = 0
        create_replacement_table()
        dataset_list = _gbif_api.get_dataset_list()
        excluded = ['https://ipt.artsdatabanken.no/archive.do?r=nbic_other', 'https://ipt.artsdatabanken.no/archive.do?r=speciesobservationsservice2']

        for dataset in dataset_list:
            dataset_details = _gbif_api.get_dataset_detailed_info(dataset['key'])
            #insert_dataset(dataset_details)
            endpoint = _gbif_api.get_dwc_endpoint(dataset_details['endpoints'])
            if not endpoint or endpoint['url'] in excluded: # I.e. it is a metadata only endpoint or we want to skip it
                continue
            for core_type, file_obj in _gbif_api.get_cores_from_ipt(endpoint['url']):
                core_id_key = _darwin_core_processing.get_core_id(core_type)
                if core_id_key:
                    count_of_added = _darwin_core_processing.copy_csv_to_replacement_table(file_obj, core_id_key, dataset['key'])
                    if count_of_added:
                        total_added += count_of_added

        if options['reset']:
            _cache_data.reset()
        else:
            _cache_data.merge_in_new_data()
        Statistic.objects.update_or_create(name='total_count', value=DarwinCoreObject.objects.count())

def create_replacement_table(): # See tests/create_db.sql for structure
    with connection.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS replacement_table')
        cursor.execute('CREATE TABLE replacement_table (LIKE website_darwincoreobject INCLUDING ALL)')

#def insert_dataset(dataset):
#    dataset['label'] = dataset['title']
#    dataset['sameas'] = dataset['doi']
#    dataset['type'] = 'dataset'
#    del dataset['title'], dataset['doi'], dataset['comments']
#    vals = (dataset['key'], json.dumps(dataset), date.today())
#    insert_sql = "INSERT INTO replacement_table(id, data, created_date) VALUES (%s, %s, %s)"
#    with p.connect('') as conn:
#        with conn.cursor() as cursor:
#            cursor.execute(insert_sql, vals)

