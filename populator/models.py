from django.db import models
from django.db.models import JSONField
from website.models import ResolvableObject


class ResolvableObjectMigration(models.Model):
    id = models.CharField(max_length=200, primary_key=True, serialize=False)
    data = JSONField()
    type = models.CharField(max_length=200)
    dataset_id = models.CharField(max_length=200)


class History(models.Model):
    resolvable_object = models.ForeignKey(ResolvableObject, on_delete=models.DO_NOTHING)
    changed_data = JSONField()
    changed_date = models.DateField()


# Need to store count of dwc objects manually as it's too time consuming to calculate on the fly
class StatisticsManager(models.Manager):
    def get_total_count(self):
        try:
            return self.get(name='total_count').value
        except self.model.DoesNotExist:
            return self.set_total_count()

    def set_total_count(self):
        statistic, created = self.update_or_create(name='total_count', value=ResolvableObject.objects.count())
        return statistic.value


class Statistic(models.Model):
    name = models.CharField(primary_key=True, max_length=100)
    value = models.IntegerField()
    objects = StatisticsManager()

