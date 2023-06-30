from rest_framework.renderers import BaseRenderer
from rdflib import Graph
import json


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

        graphed_data = Graph().parse(data=json.dumps(data), format='json-ld')
        return graphed_data.serialize()


class JSONLDRenderer(BaseRenderer):
    """
    Renderer which serializes to JSON-LD, as recommended by W3C for linked data
    """
    media_type = 'application/ld+json'
    format = 'ld+json'

    def render(self, data, accepted_media_type=None, renderer_context=None):
        """
        Renders `data` into serialized JSON-LD
        """
        if data is None:
            return ''

        return json.dumps(data)

