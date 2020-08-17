from django.db import models
from django.db.models import JSONField
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


# Need to store count of dwc objects manually as it's too time consuming to calculate on the fly
class StatisticsManager(models.Manager):
    def get_total_count(self):
        return self.get(name='total_count').value

    def set_total_count(self, value):
        self.create(name='total_count', value=value)


class Statistic(models.Model):
    name = models.CharField(primary_key=True, max_length=100)
    value = models.IntegerField()
    objects = StatisticsManager()

