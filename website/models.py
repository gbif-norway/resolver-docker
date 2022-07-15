from django.db import models
from django.db.models import JSONField
from django.contrib.postgres.indexes import GinIndex


class Dataset(models.Model):
    id = models.CharField(max_length=200, primary_key=True, serialize=False)  # This could also be a uuid field
    data = JSONField()
    created_date = models.DateField(auto_now_add=True)
    deleted_date = models.DateField(null=True, blank=True)


class ResolvableObject(models.Model):
    id = models.CharField(max_length=200, primary_key=True, serialize=False)
    parent = models.ForeignKey('self', null=True, blank=True, on_delete=models.PROTECT)
    data = JSONField()
    type = models.CharField(max_length=200)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    created_date = models.DateField(auto_now_add=True)
    deleted_date = models.DateField(null=True, blank=True)

    class Meta:
        indexes = [
            GinIndex(fields=['data'])
        ]
        # CREATE INDEX idxginscientificname ON website_resolvableobject USING GIN ((data -> 'scientificname'));
        # CREATE INDEX ro_data_gin_idx ON website_resolvableobject USING GIN (data jsonb_path_ops);
