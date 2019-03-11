from django.db import models
from django.contrib.postgres.fields import JSONField
import uuid

class DarwinCoreObject(models.Model):
    uuid = models.UUIDField(primary_key=True)
    data = JSONField()

