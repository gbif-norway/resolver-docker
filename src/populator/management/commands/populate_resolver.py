from django.core.management.base import BaseCommand, CommandError
from populator.models import Statistic, ResolvableObject
from website.models import Dataset
from populator.management.commands import _gbif_api, _migration_processing, _cache_data
import logging
from django.db import connection
from datetime import datetime


class Command(BaseCommand):
    help = 'Populates the resolver from datasets added to GBIF by Norwegian IPTs'
    logger = logging.getLogger(__name__)

    def add_arguments(self, parser):
        parser.add_argument('--reset', action='store_true', help='Resets (clears cache) from GBIF')
        parser.add_argument('--skip', action='store_true', help='Skips ingestion and goes straight to merging - use if the script failed at merging stage')

    def handle(self, *args, **options):
        dataset_list = _gbif_api.get_dataset_list()

        # Skip some datasets
        big = {
               'crop wild relatives, global': '07044577-bd82-4089-9f3a-f4a9d2170b2e',
               'artsobs': 'b124e1e0-4755-430f-9eab-894f25a9b59c',
               }

        # Set up for import
        if not options['skip']:
            create_duplicates_file()
            reset_import_table()
        dataset_ids = []
        unchanged_dataset_ids = []
        overall_start = datetime.now()

        # Iterate over GBIF datasets
        for dataset in dataset_list:
            if options['skip'] or dataset['key'] in big.values():
                self.logger.info('skip')
                continue
            start = datetime.now()

            # Get dataset details
            dataset_details = _gbif_api.get_dataset_detailed_info(dataset['key'])
            endpoint = _gbif_api.get_dwc_endpoint(dataset_details['endpoints'])
            self.logger.info(dataset_details['title'])

            if not endpoint:
                self.logger.info('Metadata only dataset, skipping')
                continue
            if not sync_dataset(dataset_details):
                self.logger.info('Dataset is unchanged, skipping')
                dataset_ids.append(dataset['key'])
                unchanged_dataset_ids.append(dataset['key'])
                continue

            self.logger.info(endpoint['url'])

            stored = _gbif_api.get_dwca_and_store_as_tmp_zip(endpoint['url'])
            if stored:
                _migration_processing.import_dwca(dataset['key'])
            else:
                self.logger.info(f"Could not download dataset {dataset['key']}")
            dataset_ids.append(dataset['key'])
            log_time(start, f"fin inserting dataset {dataset['key']}")

        log_time(overall_start, f'finished all datasets {len(dataset_ids)}, merging in starts next')

        if not options['skip']:
            start = datetime.now()
            _cache_data.sync_datasets(dataset_ids)
            log_time(start, 'caching complete')

        start = datetime.now()
        _cache_data.merge_in_new_data(skipped_datasets=unchanged_dataset_ids)
        log_time(start, 'merging complete')
        start = datetime.now()
        total_count = Statistic.objects.set_total_count()
        log_time(start, 'finished! total  count now set {}'.format(total_count))


def create_duplicates_file(file='/srv/duplicates.txt'):
    with open(file, 'w') as f:
        f.write('id|new_data|new_datasetid|new_coretype|old_data|old_datasetid\n')


def sync_dataset(dataset):
    dataset['label'] = dataset['title']
    dataset['sameas'] = dataset['doi']
    key = dataset.pop('key')
    del dataset['title'], dataset['doi']
    try:
        dataset_object = Dataset.objects.get(id=key)
        if dataset_object.data['modified'] == dataset['modified']:
            return False
        dataset_object.data = dataset
        dataset_object.save()
    except Dataset.DoesNotExist:
        dataset_object = Dataset.objects.create(id=key, data=dataset)
    return dataset_object


def reset_import_table():
    with connection.cursor() as cursor:
        cursor.execute('TRUNCATE populator_resolvableobjectmigration')


def log_time(start, message):
    logger = logging.getLogger(__name__)
    time_string = datetime.now() - start
    logger.info('{}    - time taken - {}'.format(message, str(time_string)[:7]))
