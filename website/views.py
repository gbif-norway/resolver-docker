from .models import DarwinCoreObject
from rest_framework import viewsets, renderers
from .serializers import DarwinCoreObjectSerializer
from .renderers import RDFRenderer, JSONLDRenderer
from .paginators import CustomPagination


class DarwinCoreObjectViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GBIF Norway's resolver provides darwin core object data from datasets published to gbif.org by Norwegian publishers
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = DarwinCoreObject.objects.all()
    serializer_class = DarwinCoreObjectSerializer
    pagination_class = CustomPagination

    def get_queryset(self):
        sci_name = self.request.query_params.get('data__scientificname')
        if sci_name:
            return self.queryset.filter(data__scientificname=sci_name)
        else:
            return self.queryset

