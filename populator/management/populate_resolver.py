import gbif_api
import darwin_core_processing
import cache_data
from datetime import date
import json
import psycopg2 as p
import logging
import sys

def populate_resolver(reset = False):
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logging.info('Resolver import started')
    total_added = 0
    create_replacement_table()
    dataset_list = gbif_api.get_dataset_list()
    for dataset in dataset_list:
        print('start of dataset loop')
        dataset_details = gbif_api.get_dataset_detailed_info(dataset['key'])
        insert_dataset(dataset_details)
        darwin_core_endpoint = gbif_api.get_dwc_endpoint(dataset_details['endpoints'])
        if not darwin_core_endpoint: # I.e. it is a metadata only endpoint
            continue
        if 'https://ipt.artsdatabanken.no/archive.do?r=nbic_other' == darwin_core_endpoint['url']:
            continue
        if 'speciesobservationsservice2' in darwin_core_endpoint['url']:
            continue
        if 'https://ipt.nina.no/archive.do?r=butterflies_bumblebees2020' == darwin_core_endpoint['url']:
            continue
        cores = gbif_api.get_cores_from_ipt(darwin_core_endpoint['url'])
        for core_type, file_obj in cores:
            core_id_key = darwin_core_processing.get_core_id(core_type)
            if core_id_key:
                print('new core ' + core_type)
                count_of_added = darwin_core_processing.copy_csv_to_replacement_table(file_obj, core_id_key, dataset['key'])
                if count_of_added:
                    total_added += count_of_added
                    logging.info('%s items added for %s - %s' % (count_of_added, core_type, darwin_core_endpoint['url']))
                    print('%s items added for %s - %s' % (count_of_added, core_type, darwin_core_endpoint['url']))
                else:
                    logging.warning('No items added for %s - %s' % (core_type, darwin_core_endpoint['url']))
            else:
                logging.warning('Core type not supported: %s - %s' % (core_type, darwin_core_endpoint['url']))

    logging.info('Resolver import complete: total number of rows imported %s' % total_added)
    print('Resolver import complete: total number of rows imported %s' % total_added)

    # At this stage there is a replacement_table with all records needing to populate the resolver.
    if reset or get_dwc_count() < 500: # If there is nothing currently existing in the table, treat as a reset
        print('RESETTING FROM GBIF')
        cache_data.reset()
    else:
        cache_data.merge_in_new_data()

    with p.connect('') as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO website_statistic VALUES ('total_count', %s)" % get_dwc_count())

def get_dwc_count():
    with p.connect('') as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM website_darwincoreobject')
            return cursor.fetchone()[0]

def create_replacement_table(): # See tests/create_db.sql for structure
    with p.connect('') as conn:
        with conn.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS replacement_table')
            cursor.execute('CREATE TABLE replacement_table (LIKE website_darwincoreobject INCLUDING ALL)')

def insert_dataset(dataset):
    dataset['label'] = dataset['title']
    dataset['sameas'] = dataset['doi']
    dataset['type'] = 'dataset'
    del dataset['title'], dataset['doi'], dataset['comments']
    vals = (dataset['key'], json.dumps(dataset), date.today())
    insert_sql = "INSERT INTO replacement_table(id, data, created_date) VALUES (%s, %s, %s)"
    with p.connect('') as conn:
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, vals)

if __name__ == "__main__":
    populate_resolver(len(sys.argv) > 1 and sys.argv[1] == 'reset')
