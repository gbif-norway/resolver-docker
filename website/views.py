from .models import DarwinCoreObject
from rest_framework import viewsets, renderers
from .serializers import DarwinCoreObjectSerializer
from .renderers import RDFRenderer, JSONLDRenderer
from .paginators import CustomPagination
from django.db.models import Q


class DarwinCoreObjectViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GBIF Norway's resolver provides data published to gbif.org by Norwegian publishers. Query by appending e.g. `?data__type=dataset` to get a list of all datasets, or `?data__scientificname=Galium+odoratum` to filter on scientific name.
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = DarwinCoreObject.objects.all()
    serializer_class = DarwinCoreObjectSerializer
    pagination_class = CustomPagination

    def get_queryset(self):
        q_objects = Q()
        for field_name, search_term in self.request.query_params.items():
            if field_name[:6] == 'data__':
                q_objects &= Q(**{field_name:search_term})
        return DarwinCoreObject.objects.filter(q_objects)

