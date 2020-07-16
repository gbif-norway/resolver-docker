from django.db import models
from django.contrib.postgres.fields import JSONField
import uuid

class DarwinCoreObject(models.Model):
    id = models.CharField(primary_key=True, max_length=200)
    data = JSONField()
    deleted_date = models.DateField(null=True, blank=True)

class History(models.Model):
    darwin_core_object = models.ForeignKey(DarwinCoreObject, on_delete=models.CASCADE)
    changed_data = JSONField()
    changed_date = models.DateField()
