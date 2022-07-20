from .models import ResolvableObject, Dataset
from populator.models import History
from rest_framework import viewsets, renderers, pagination
from .serializers import ResolvableObjectSerializer, DatasetSerializer, HistorySerializer
from .renderers import RDFRenderer, JSONLDRenderer
from .paginators import CustomPagination, CustomCountPagination
from django_filters.rest_framework import DjangoFilterBackend
from collections import defaultdict
import json


class HistoryViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Search through past changes made to GBIF Norway resolver records.
    Every time a new ingestion of data is made, the provenance is stored and is accessible here.
    Query changed_data with changed_data__has_key=searchkey, or changed_data__contains={"key1":"value1","key2":"value2"}, see https://docs.djangoproject.com/en/4.0/ref/contrib/postgres/fields/#has-key
    Search for changes made to a particular resolvable object with resolvable_object=[uuid]
    Search for changes recorded on (__equals), before (__lte) or after (__gte) a particular ingestion date with e.g. changed_date__gte=2022-07-01

    Example query, searches for changes registered on 2022-07-01 where the dwc:type was changed from PhysicalObject, and there was a change to the dwc:catalognumber field:
    ?changed_date__gte=2022-07-01&changed_data__contains={"type": "PhysicalObject"}&changed_data__has_key=catalognumber
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = History.objects.all()
    serializer_class = HistorySerializer
    filter_backends = [DjangoFilterBackend]
    filter_fields = { 'changed_date': ['gte', 'lte', 'exact'] }

    def get_queryset(self):
        query_params = self.request.query_params
        changed_data = { key.replace('changed_data__', ''): item for key, item in query_params.items() if 'changed_data__' in key }
        query = {}
        for key, item in changed_data.items():
            if key == 'has_key':
                query['changed_data__has_key'] = item
            elif key == 'contains':
                query['changed_data__contains'] = json.loads(item)  # Item should be {"key": "value", ...}
            else:
                continue
        if 'resolvable_object' in query_params:
            query['resolvable_object_id'] = query_params['resolvable_object']
        return History.objects.filter(**query)


class ResolvableObjectViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GBIF Norway's resolver provides data published to gbif.org by Norwegian publishers. Query by appending e.g.
    `?scientificname=Galium+odoratum` to filter on scientific name. Add '_add_counts=true' to return the result count (not performant).
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = ResolvableObject.objects.all()
    serializer_class = ResolvableObjectSerializer
    pagination_class = CustomPagination
    filter_backends = [DjangoFilterBackend]
    filter_fields = { 'deleted_date': ['gte', 'lte', 'exact'], 'dataset_id': ['exact'] }

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
