from rest_framework import serializers
from .models import DarwinCoreObject

class DarwinCoreObjectSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = DarwinCoreObject
        fields = ('data',)

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        ret = ret['data']
        prefixed_object = {'dwc:%s' % key: value for key, value in ret.items()}
        prefixed_object['@id'] = 'http://purl.org/gbifnorway/id/%s' % ret['id']
        prefixed_object['@context'] = {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/'}
        return prefixed_object
