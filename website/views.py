from .models import DarwinCoreObject
from rest_framework import viewsets, renderers
from .serializers import DarwinCoreObjectSerializer
from .renderers import RDFRenderer, JSONLDRenderer
from .paginators import CustomPagination
from rest_framework.pagination import LimitOffsetPagination


class DarwinCoreObjectViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GBIF Norway's resolver provides data published to gbif.org by Norwegian publishers. Query by appending e.g. `?type=dataset` to get a list of all datasets, or `?scientificname=Galium+odoratum` to filter on scientific name.
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = DarwinCoreObject.objects.all()
    serializer_class = DarwinCoreObjectSerializer
    pagination_class = CustomPagination

    def get_queryset(self):
        query_params = {key: item for key, item in self.request.query_params.items() if key not in ['offset', 'limit']}
        return DarwinCoreObject.objects.filter(data__contains=query_params)

