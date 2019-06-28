from rest_framework.renderers import BaseRenderer
from rdflib import Graph
from django.forms.models import model_to_dict
import json

def _json(json_object):
    json_object = json_object['data']
    prefixed_object = {'dwc:%s' % key: value for key, value in json_object.items()}
    prefixed_object['@id'] = 'http://purl.org/gbifnorway/id/%s' % json_object['id']
    prefixed_object['@context'] = {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/'}
    return prefixed_object


class RDFRenderer(BaseRenderer):
    """
    Renderer which serializes to RDF - the Resource Descriptor Framework
    """
    media_type = 'application/rdf+xml'
    format = 'rdf+xml'

    def render(self, data, accepted_media_type=None, renderer_context=None):
        """
        Renders `data` into serialized RDF+XML
        """
        if data is None:
            return ''

        json_object = _json(data)
        graphed_data = Graph().parse(data=json.dumps(json_object), format='json-ld')
        return graphed_data.serialize()

