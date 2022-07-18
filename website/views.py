from .models import ResolvableObject, Dataset
from populator.models import History
from rest_framework import viewsets, renderers, pagination
from .serializers import ResolvableObjectSerializer, DatasetSerializer, HistorySerializer
from .renderers import RDFRenderer, JSONLDRenderer
from .paginators import CustomPagination, CustomCountPagination
from django_filters.rest_framework import DjangoFilterBackend


class HistoryViewSet(viewsets.ReadOnlyModelViewSet):
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = History.objects.all()
    serializer_class = HistorySerializer
    filter_backends = [DjangoFilterBackend]
    filter_fields = ('resolvable_object', 'changed_date')

    def get_queryset(self):
        query_params = self.request.query_params
        changed_data_args = { key.replace('changed_data__', ''): item for key, item in query_params.items() if 'changed_data__' in key }
        args = {'changed_data__contains': changed_data_args}
        return History.objects.filter(**args)


class ResolvableObjectViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GBIF Norway's resolver provides data published to gbif.org by Norwegian publishers. Query by appending e.g.
    `?scientificname=Galium+odoratum` to filter on scientific name. Add '_add_counts=true' to return the result count (not performant).
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = ResolvableObject.objects.all()
    serializer_class = ResolvableObjectSerializer
    pagination_class = CustomPagination

    def get_queryset(self):
        query_params = self.request.query_params
        data_args = {key: item for key, item in query_params.items() if key not in ['offset', 'limit', 'format', 'type', '_add_counts']}
        args = {'data__contains': data_args}
        if 'type' in query_params:
            args['type'] = query_params['type']
        if '_add_counts' in query_params and query_params['_add_counts'] == 'true':
            self.pagination_class = CustomCountPagination
        return ResolvableObject.objects.filter(**args)


class DatasetViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Datasets endpoint
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = Dataset.objects.all()
    serializer_class = DatasetSerializer
    pagination_class = pagination.LimitOffsetPagination
