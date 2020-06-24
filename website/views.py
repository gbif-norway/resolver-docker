from .models import DarwinCoreObject
from rest_framework import viewsets, renderers, pagination
from .serializers import DarwinCoreObjectSerializer
from .renderers import RDFRenderer, JSONLDRenderer
from .paginators import CustomPagination


class DarwinCoreObjectViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GBIF Norway's resolver provides data published to gbif.org by Norwegian publishers. Query by appending e.g. `?type=dataset` to get a list of all datasets, or `?scientificname=Galium+odoratum` to filter on scientific name.
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = DarwinCoreObject.objects.all()
    serializer_class = DarwinCoreObjectSerializer
    pagination_class = CustomPagination

    def get_queryset(self):
        return DarwinCoreObject.objects.filter(data__contains=self.request.query_params)

