from rest_framework import serializers
from .models import ResolvableObject

class ResolvableObjectSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = ResolvableObject
        fields = ('data',)

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        ret = ret['data']
        prefixed_object = {'dwc:%s' % key: value for key, value in ret.items()}
        prefixed_object['@context'] = {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/', 'owl': 'https://www.w3.org/tr/owl-ref/'}

        if 'dwc:id' in prefixed_object:
            prefixed_object['@id'] = 'http://purl.org/gbifnorway/id/%s' % prefixed_object['dwc:id']
            prefixed_object['owl:sameas'] = prefixed_object['dwc:id']
            del prefixed_object['dwc:id']

        if 'dwc:sameas' in prefixed_object:
            if 'dwc:type' in prefixed_object and prefixed_object['dwc:type'] == 'dataset':
                prefixed_object['owl:sameas'] = prefixed_object['dwc:sameas']
            del prefixed_object['dwc:sameas']

        if 'dwc:type' in prefixed_object:
            prefixed_object['dc:type'] = prefixed_object['dwc:type']
            del prefixed_object['dwc:type']

        if 'dwc:label' in prefixed_object:
            prefixed_object['rdfs:label'] = prefixed_object['dwc:label']
            del prefixed_object['dwc:label']
            prefixed_object['@context']['rdfs'] = 'https://www.w3.org/tr/rdf-schema/'

        return prefixed_object
