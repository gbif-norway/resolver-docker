from django.core.management.base import BaseCommand, CommandError
from django.core import mail
from website.models import DarwinCoreObject
from website.management.commands import _gbif_api
from website.management.commands import _darwin_core_processing
from django.db import connection
import datetime

class Command(BaseCommand):
    help = 'Populates the resolver from datasets added to GBIF by Norwegian IPTs'

    def handle(self, *args, **options):
        total_added = 0
        self._create_replacement_table()
        for dataset in _gbif_api.get_dataset_list():
            endpoints = _gbif_api.get_dataset_endpoints(dataset['key'])
            darwin_core_endpoint = _gbif_api.get_dwc_endpoint(endpoints)

            cores = _gbif_api.get_cores_from_ipt(darwin_core_endpoint['url'])
            for core_type, file_obj in cores:
                core_id_key = _darwin_core_processing.get_core_id(core_type)
                if core_id_key:
                    count_of_added = _darwin_core_processing.copy_csv_to_replacement_table(file_obj, core_id_key)
                    if count_of_added:
                        total_added += count_of_added
                    else:
                        self._email_admins('No items added for %s' % (core_type), 'File : %s' % (darwin_core_endpoint['url']))
                else:
                    self._email_admins('Core type not supported: %s' % (core_type), 'File url: %s' % (darwin_core_endpoint['url']))

        self._email_admins('Resolver import complete %s' % datetime.datetime.now().strftime("%Y-%m-%d"), 'Total number of rows imported %s' % total_added)

        # At this stage there is a replacement_table with all records needing to populate the resolver.
        with connection.cursor() as cursor:
            drop_table_sql = 'DROP TABLE IF EXISTS website_darwincoreobject'
            cursor.execute(drop_table_sql)
            rename_table_sql = 'ALTER TABLE replacement_table RENAME TO website_darwincoreobject'
            cursor.execute(rename_table_sql)

    def _email_admins(self, subject, message):
        mail.mail_admins(subject, message, fail_silently=True)

    def _create_replacement_table(self):
        with connection.cursor() as cursor:
            drop_table_sql = 'DROP TABLE IF EXISTS replacement_table'
            cursor.execute(drop_table_sql)
            create_table_sql = 'CREATE TABLE replacement_table (uuid uuid, data jsonb)'
            cursor.execute(create_table_sql)
