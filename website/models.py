from django.db import models
from django.contrib.postgres.fields import JSONField
import uuid

class DarwinCoreObject(models.Model):
    id = models.CharField(primary_key=True, max_length=200)
    data = JSONField()
