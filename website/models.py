from django.db import models
from django.contrib.postgres.fields import JSONField
from django.contrib.postgres.indexes import GinIndex
import uuid

class DarwinCoreObject(models.Model):
    id = models.CharField(primary_key=True, max_length=200)
    data = JSONField()
    deleted_date = models.DateField(null=True, blank=True)
    created_date = models.DateField(auto_now_add=True)

    class Meta:
        indexes = [GinIndex(fields=['data'])]
        # CREATE INDEX idxginscientificname ON website_darwincoreobject USING GIN ((data -> 'scientificname'));


class History(models.Model):
    darwin_core_object = models.ForeignKey(DarwinCoreObject, on_delete=models.DO_NOTHING)
    changed_data = JSONField()
    changed_date = models.DateField()
