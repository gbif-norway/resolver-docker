from rest_framework import serializers
from .models import DarwinCoreObject

class DarwinCoreObjectSerializer(serializers.ModelSerializer):
    class Meta:
        model = DarwinCoreObject
        fields = ('data',)
