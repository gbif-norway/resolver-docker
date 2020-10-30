from .models import ResolvableObject
from rest_framework import viewsets, renderers
from .serializers import ResolvableObjectSerializer
from .renderers import RDFRenderer, JSONLDRenderer
from .paginators import CustomPagination
from rest_framework.pagination import LimitOffsetPagination


class ResolvableObjectViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GBIF Norway's resolver provides data published to gbif.org by Norwegian publishers. Query by appending e.g.
    `?type=dataset` to get a list of all datasets, or `?scientificname=Galium+odoratum` to filter on scientific name.
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = ResolvableObject.objects.all()
    serializer_class = ResolvableObjectSerializer
    pagination_class = CustomPagination

    def get_queryset(self):
        query_params = {key: item for key, item in self.request.query_params.items() if key not in ['offset', 'limit', 'format']}
        return ResolvableObject.objects.filter(data__contains=query_params)

