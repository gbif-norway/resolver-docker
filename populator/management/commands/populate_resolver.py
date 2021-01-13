from django.core.management.base import BaseCommand, CommandError
from populator.models import Statistic, ResolvableObject
from website.models import Dataset
from populator.management.commands import _gbif_api, _migration_processing, _cache_data
import json
import logging
from django.db import connection
import time


class Command(BaseCommand):
    help = 'Populates the resolver from datasets added to GBIF by Norwegian IPTs'

    def add_arguments(self, parser):
        parser.add_argument('--reset', action='store_true', help='Resets (clears cache) from GBIF')

    def handle(self, *args, **options):
        logger = logging.getLogger(__name__)
        dataset_list = _gbif_api.get_dataset_list()
        #dataset_list = []

        # Skip some datasets
        big = {
               'crop wild relatives, global': '07044577-bd82-4089-9f3a-f4a9d2170b2e',
               'artsobs': 'b124e1e0-4755-430f-9eab-894f25a9b59c',
               }
        skip = False

        # Set up for import
        create_duplicates_file()
        reset_import_table()
        dataset_ids = []

        overall_start = time.time()
        # Iterate over GBIF datasets
        for dataset in dataset_list:
            if skip or dataset['key'] in big.values():
                logger.info('skip')
                continue
            start = time.time()

            # Get dataset details
            dataset_details = _gbif_api.get_dataset_detailed_info(dataset['key'])
            endpoint = _gbif_api.get_dwc_endpoint(dataset_details['endpoints'])

            if not endpoint:
                logger.info('Metadata only dataset, skipping')
                continue
            if not sync_dataset(dataset_details):
                logger.info('Dataset is unchanged, skipping')
                continue

            logger.info(endpoint['url'])
            _gbif_api.get_dwca_and_store_as_tmp_zip(endpoint['url'])
            _migration_processing.import_dwca(dataset['key'])
            dataset_ids.append(dataset['key'])
            logger.info('fin inserting dataset, took {} -- {}'.format(dataset['key'], time.time() - start))

        logger.info('total time: {}, merging in starts next'.format(time.time() - overall_start))
        start = time.time()
        _cache_data.sync_datasets(dataset_ids)
        logger.info('caching complete {}'.format(time.time() - start))
        start = time.time()
        _cache_data.merge_in_new_data(False)  # options['reset']
        logger.info('merging complete {}'.format(time.time() - start))
        total_count = Statistic.objects.set_total_count()
        logger.info('finished - total count now set: {}'.format(total_count))


def create_duplicates_file(file='/code/duplicates.txt'):
    with open(file, 'w') as f:
        f.write('id|new_data|new_datasetid|new_coretype|old_data|old_datasetid\n')


def sync_dataset(dataset):
    dataset['label'] = dataset['title']
    dataset['sameas'] = dataset['doi']
    del dataset['title'], dataset['doi']
    try:
        dataset_object = Dataset.objects.get(id=dataset['key'])
        if dataset_object.data['modified'] == dataset['modified']:
            return False
        dataset_object.data = dataset
        dataset_object.save()
    except Dataset.DoesNotExist:
        dataset_object = Dataset.objects.create(id=dataset['key'], data=dataset)
    return dataset_object


def reset_import_table():
    with connection.cursor() as cursor:
        cursor.execute('TRUNCATE populator_resolvableobjectmigration')

