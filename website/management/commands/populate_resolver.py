from django.core.management.base import BaseCommand, CommandError
from django.core import mail
from website.models import DarwinCoreObject
from website.management.commands import _gbif_api
from website.management.commands import _darwin_core_processing

class Command(BaseCommand):
    help = 'Populates the resolver from datasets added to GBIF by Norwegian IPTs'

    def handle(self, *args, **options):
        total_added = 0
        for dataset in _gbif_api.get_dataset_list():
            endpoints = _gbif_api.get_dataset_endpoints(dataset['key'])
            darwin_core_endpoint = _gbif_api.get_first_darwin_core_url_from_list(endpoints)

            with _gbif_api.get_cores_from_ipt(darwin_core_endpoint['url']) as cores:
                for core_type, file_obj in cores:
                    core_id_key = _darwin_core_processing.get_core_id(core_type)
                    if core_id_key:
                        darwin_core_objects = _darwin_core_processing.build_darwin_core_objects(core_id_key, file_obj)
                        DarwinCoreObject.objects.bulk_create(darwin_core_objects)
                        total_added += len(darwin_core_objects)
                    else:
                        _email_error('Core ID not supported %s' % (core_type), 'File : %s' % (darwin_core_endpoint['url']))

    def _email_error(e, subject):
        exc_info = sys.exc_info()
        mail.mail_admins(subject, '\n'.join(traceback.format_exception(*exc_info)), fail_silently=True)

