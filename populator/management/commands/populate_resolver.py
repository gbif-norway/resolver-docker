from django.core.management.base import BaseCommand, CommandError
from populator.models import Statistic, ResolvableObject
from website.models import Dataset
from populator.management.commands import _gbif_api, _migration_processing, _cache_data
from django.db import connection
import json
import os


class Command(BaseCommand):
    help = 'Populates the resolver from datasets added to GBIF by Norwegian IPTs'

    def add_arguments(self, parser):
        parser.add_argument('--reset', action='store_true', help='Resets (clears cache) from GBIF')

    def handle(self, *args, **options):
        total_added = 0
        dataset_list = _gbif_api.get_dataset_list()
        excluded = ['https://ipt.artsdatabanken.no/archive.do?r=nbic_other', 'https://ipt.artsdatabanken.no/archive.do?r=speciesobservationsservice2', 'https://ipt.gbif.no/archive.do?r=dnv', 'https://ipt.gbif.no/archive.do?r=artsnavn']
        skip = False
        create_duplicates_file()

        for dataset in dataset_list:
            dataset_details = _gbif_api.get_dataset_detailed_info(dataset['key'])
            endpoint = _gbif_api.get_dwc_endpoint(dataset_details['endpoints'])
            if not endpoint or endpoint['url'] in excluded: # I.e. it is a metadata only endpoint or we want to skip it
                continue
            if endpoint['url'] == 'https://ipt.nina.no/archive.do?r=fiskmerk':
                skip = False
                continue
            if skip:
                continue
            print(endpoint['url'])
            insert_dataset(dataset)
            for core_type, file_obj in _gbif_api.get_cores_from_ipt(endpoint['url']):
                total_added += _migration_processing.copy_csv_to_migration_table(file_obj, core_type, dataset['key'])

        _cache_data.merge_in_new_data(options['reset'])
        Statistic.objects.update_or_create(name='total_count', value=ResolvableObject.objects.count())


def create_duplicates_file(file='/code/duplicates.txt'):
    with open(file, 'w') as f:
        f.write('id|new_data|new_datasetid|new_coretype|old_data|old_datasetid\n')


def insert_dataset(dataset):
    dataset['label'] = dataset['title']
    dataset['sameas'] = dataset['doi']
    del dataset['title'], dataset['doi'], dataset['comments']
    Dataset.objects.get_or_create(id=dataset['key'], data=json.dumps(dataset))
