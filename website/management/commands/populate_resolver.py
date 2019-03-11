from django.core.management.base import BaseCommand, CommandError
from website.models import DarwinCoreObject
import requests

class Command(BaseCommand):
    help = 'Populates the resolver from datasets added to GBIF by Norwegian IPTs'

    def handle(self, *args, **options):
        self.stdout.write('test')
