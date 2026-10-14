from rest_framework.renderers import BaseRenderer
from rdflib import Graph
import json
import re

# XML 1.0 cannot represent these, not even as character references, but some records have them (e.g. vertical tabs used as line breaks)
XML_ILLEGAL_CHARACTERS = re.compile('[\x00-\x08\x0b\x0c\x0e-\x1f\ufffe\uffff]')


class RDFRenderer(BaseRenderer):
    """
    Renderer which serializes JSON-LD to RDF/XML - the Resource Description Framework
    """
    media_type = 'application/rdf+xml'
    format = 'rdf+xml'

    def render(self, data, accepted_media_type=None, renderer_context=None):
        """
        Renders `data` into serialized RDF/XML
        """
        if data is None:
            return ''

        # Lists and shared UUIDs wrap the records in 'results', which is not JSON-LD, so graph the records themselves
        if isinstance(data, dict) and 'results' in data:
            data = data['results']
        return self.serialize(Graph().parse(data=json.dumps(data), format='json-ld'))

    def serialize(self, graph):
        # Only literal values can contain these, so replacing them in the output keeps the XML well-formed
        return XML_ILLEGAL_CHARACTERS.sub('\ufffd', graph.serialize(format='xml'))


class TurtleRenderer(RDFRenderer):
    """
    Renderer which serializes JSON-LD to Turtle, which unlike RDF/XML can represent every value
    """
    media_type = 'text/turtle'
    format = 'ttl'

    def serialize(self, graph):
        return graph.serialize(format='turtle')


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
