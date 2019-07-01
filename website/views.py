from .models import DarwinCoreObject
from rest_framework import viewsets, renderers
from .serializers import DarwinCoreObjectSerializer
from .renderers import RDFRenderer, JSONLDRenderer


class DarwinCoreObjectViewSet(viewsets.ReadOnlyModelViewSet):
    """
    A ViewSet for reading Darwin Core Objects
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = DarwinCoreObject.objects.all()
    serializer_class = DarwinCoreObjectSerializer

