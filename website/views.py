from django.shortcuts import get_object_or_404, render
from django.http import HttpResponse, JsonResponse
from .models import DarwinCoreObject
from django.core import serializers
from rdflib import Graph
from django.forms.models import model_to_dict
import json

def index(request):
    return HttpResponse('Resolver')

def detail(request, uuid):
    darwin_core_object = get_object_or_404(DarwinCoreObject, uuid=uuid)

    mime_types = {
     '*/*': _html(request, darwin_core_object),
     'text/html': _html(request, darwin_core_object),
     'application/json': JsonResponse(_json(darwin_core_object)),
     'application.ld+json': JsonResponse(_json(darwin_core_object)),
     'text/plain': _text(request, darwin_core_object),
     'text/n3': _n3(darwin_core_object),
     'text/turtle': _n3(darwin_core_object),
     'text/xml': _rdf(darwin_core_object),
     'application/xml': _rdf(darwin_core_object),
     'application/rdf+xml': _rdf(darwin_core_object)
    }

    content_type = request.META.get("HTTP_ACCEPT")
    if content_type in mime_types:
        return mime_types[content_type]
    else:
        return HttpResponse('Unrecognised MIME type in request headers')

def _html(request, darwin_core_object):
    return render(request, 'detail.html', {'darwin_core_object': darwin_core_object})

def _text(request, darwin_core_object):
    return render(request, 'detail.html', {'darwin_core_object': darwin_core_object}, content_type='text/plain')

def _json(darwin_core_object):
    json_object = model_to_dict(darwin_core_object)['data']
    prefixed_object = {'dwc:%s' % key: value for key, value in json_object.items()}
    prefixed_object['@id'] = 'http://purl.org/gbifnorway/id/%s' % darwin_core_object.uuid
    prefixed_object['@context'] = {'dc': 'http://purl.org/dc/elements/1.1/', 'dwc': 'http://rs.tdwg.org/dwc/terms/'}
    return prefixed_object

def graph(darwin_core_object):
    json_object = _json(darwin_core_object)
    return Graph().parse(data=json.dumps(json_object), format='json-ld')

def _n3(darwin_core_object):
    return HttpResponse(graph(darwin_core_object).serialize(), content_type='text/n3')

def _rdf(darwin_core_object):
    return HttpResponse(graph(darwin_core_object).serialize(), content_type='application/rdf+xml')

