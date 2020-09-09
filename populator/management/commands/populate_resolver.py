from django.core.management.base import BaseCommand, CommandError
from populator.models import Statistic, ResolvableObject
from website.models import Dataset
from populator.management.commands import _gbif_api, _migration_processing, _cache_data
import json
import logging


class Command(BaseCommand):
    help = 'Populates the resolver from datasets added to GBIF by Norwegian IPTs'

    def add_arguments(self, parser):
        parser.add_argument('--reset', action='store_true', help='Resets (clears cache) from GBIF')

    def handle(self, *args, **options):
        logger = logging.getLogger(__name__)
        dataset_list = _gbif_api.get_dataset_list()
        big = ['aea17af8-5578-4b04-b5d3-7adf0c5a1e60', 'b124e1e0-4755-430f-9eab-894f25a9b59c', 'bed78790-cbec-44af-82af-fe78e9692287', '492d63a8-4978-4bc7-acd8-7d0e3ac0e744', 'd34ed8a4-d3cb-473c-a11c-79c5fec4d649', 'e45c7d91-81c6-4455-86e3-2965a5739b1f', '2e4cc37b-302e-4f1b-bbbb-1f674ff90e14', '26f5b360-8770-4d54-9c2d-397798a5e513']
        big = []
        create_duplicates_file()

        for dataset in dataset_list:
            if dataset['key'] in big:
                continue
            dataset_details = _gbif_api.get_dataset_detailed_info(dataset['key'])
            endpoint = _gbif_api.get_dwc_endpoint(dataset_details['endpoints'])
            if not endpoint:  # I.e. it is a metadata only endpoint or we want to skip it
                continue
            logger.info(endpoint['url'])
            insert_dataset(dataset_details)
            _gbif_api.get_dwca_and_store_as_tmp_zip(endpoint['url'])
            _migration_processing.import_dwca(dataset['key'])

        logger.info('merging in')
        _cache_data.merge_in_new_data(options['reset'])
        Statistic.objects.set_total_count(ResolvableObject.objects.count())


def create_duplicates_file(file='/code/duplicates.txt'):
    with open(file, 'w') as f:
        f.write('id|new_data|new_datasetid|new_coretype|old_data|old_datasetid\n')


def insert_dataset(dataset):
    dataset['label'] = dataset['title']
    dataset['sameas'] = dataset['doi']
    del dataset['title'], dataset['doi']
    Dataset.objects.get_or_create(id=dataset['key'], data=json.dumps(dataset))
