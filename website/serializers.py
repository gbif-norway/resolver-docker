from rest_framework import serializers
from .models import ResolvableObject, Dataset
from populator.models import History


class HistorySerializer(serializers.ModelSerializer):
    class Meta:
        model = History
        fields = ('resolvable_object_id', 'changed_data', 'changed_date')


class DatasetSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Dataset
        fields = ('data', 'id', 'created_date', 'deleted_date')

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        data = ret.pop('data')
        return {**data, **ret}


class ResolvableObjectSerializer(serializers.ModelSerializer):
    dataset = DatasetSerializer()

    class Meta:
        model = ResolvableObject
        fields = ('data', 'type', 'dataset', 'deleted_date')

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        items = ret['data']
        obj = {'@id': f'http://purl.org/gbifnorway/id/{instance.id}'}
        obj['@context'] = {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/', 'owl': 'https://www.w3.org/tr/owl-ref/', 'rdf': 'https://www.w3.org/tr/rdf-schema/'}

        # Preserved specimens are material samples, so only show relevant information for these records
        if 'basisofrecord' in items and (items['basisofrecord'].lower() == 'preservedspecimen' or items['basisofrecord'].lower() == 'materialsample'):
            obj['rdf:type'] = 'materialsample'
            allowed_fields = ['scientificname', 'catalognumber', 'basisofrecord']
            for field in allowed_fields:
                if field in items:
                    obj[f'dwc:{field}'] = items[field]
        elif 'basisofrecord' in items and items['basisofrecord'].lower() == '':
            pass
        else:
            obj = {f'dwc:{key}': value for key, value in items.items()}
            if 'dwc:id' in obj:
                obj['owl:sameas'] = obj['dwc:id']
                del obj['dwc:id']
            obj['rdf:type'] = ret['type']

        if 'dwc:parent_id' in obj:
            obj['dwc:relatedResourceID'] = obj['dwc:parent_id']
            del obj['dwc:parent_id']
            if ret['type'] == 'measurementorfact':
                obj['dwc:relatedResourceID'] = f"http://purl.org/gbifnorway/id/{obj['dwc:relatedResourceID']}"
                obj['dwc:relationshipOfResource'] = 'measurement of'

        if 'dwc:sameas' in obj:
            if 'rdf:type' in obj and obj['rdf:type'] == 'dataset':
                obj['owl:sameas'] = obj['dwc:sameas']
            del obj['dwc:sameas']

        if 'dwc:label' in obj:
            obj['rdfs:label'] = obj['dwc:label']
            del obj['dwc:label']
            obj['@context']['rdfs'] = 'https://www.w3.org/tr/rdf-schema/'

        # Internal information
        dataset = ret['dataset']
        obj['dc:isPartOf'] = {'label': dataset['label'], 'id': f"https://gbif.org/dataset/{dataset['id']}", 'gbif-dataset-type': dataset['type']}
        if instance.deleted_date:
            obj['dc:dateRemoved'] = ret['deleted_date']

        return obj

