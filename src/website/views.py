from .models import ResolvableObject, Dataset
from populator.models import History
from rest_framework import viewsets, renderers, pagination
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import ResolvableObjectSerializer, DatasetSerializer, HistorySerializer
from .renderers import RDFRenderer, JSONLDRenderer
from .paginators import CustomPagination, CustomCountPagination
from django_filters.rest_framework import DjangoFilterBackend
from collections import defaultdict
import json
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from django.shortcuts import get_object_or_404
from django.http import Http404, HttpResponseRedirect
from urllib.parse import quote


GBIF_API_OCCURRENCE_URL = "https://api.gbif.org/v1/occurrence/{}"


class TemporaryGbifApiError(Exception):
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout, TemporaryGbifApiError)
    ),
    reraise=True,
)
def fetch_gbif_occurrence(gbif_key):
    response = requests.get(GBIF_API_OCCURRENCE_URL.format(gbif_key), timeout=10)
    if response.status_code == 404:
        return None
    if 500 <= response.status_code < 600:
        raise TemporaryGbifApiError("GBIF API temporary failure")
    if response.status_code != 200:
        return None
    return response.json()


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
    filter_fields = { 'deleted_date': ['gte', 'lte', 'exact'], 'dataset_id': ['exact'], 'type': ['exact'] }
    lookup_field = 'id__iexact'

    def get_queryset(self):
        query_params = self.request.query_params
        non_data_fields =['offset', 'limit', 'format', 'type', '_add_counts', 'dataset_id', 'deleted_date', 'deleted_date__gte', 'deleted_date__lte', 'type']
        data_args = {key: item for key, item in query_params.items() if key not in non_data_fields }
        args = { 'data__contains': data_args }

        if '_add_counts' in query_params and query_params['_add_counts'] == 'true':
            self.pagination_class = CustomCountPagination
        return ResolvableObject.objects.filter(**args)

    def get_object(self):
        queryset = self.filter_queryset(self.get_queryset())

        # Perform the lookup filtering.
        lookup_url_kwarg = self.lookup_url_kwarg or self.lookup_field

        assert lookup_url_kwarg in self.kwargs, (
                'Expected view %s to be called with a URL keyword argument '
                'named "%s". Fix your URL conf, or set the `.lookup_field` '
                'attribute on the view correctly.' %
                (self.__class__.__name__, lookup_url_kwarg)
        )

        # Remove urn:uuid:prefix
        filter_kwargs = {self.lookup_field: self.kwargs[lookup_url_kwarg].replace('urn:uuid:', '')}
        # obj = get_object_or_404(queryset, **filter_kwargs) - this takes too long and hangs as of 2024-04-21
        try:
            print(f'getting object with {filter_kwargs} with {filter_kwargs["id__iexact"].lower}')
            obj = ResolvableObject.objects.get(id=filter_kwargs['id__iexact'].lower())
            print(f'successfully got object {obj.id} with lowercase lookup')
        except ResolvableObject.DoesNotExist:
            try:
                print(f'getting object with {filter_kwargs}')
                obj = ResolvableObject.objects.get(id=filter_kwargs['id__iexact'])
                print(f'successfully got object {obj.id}')
            except ResolvableObject.DoesNotExist:
                raise Http404("Object does not exist")

        # May raise a permission denied
        self.check_object_permissions(self.request, obj)

        return obj

class DatasetViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Datasets endpoint
    """
    renderer_classes = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer, JSONLDRenderer, RDFRenderer)
    queryset = Dataset.objects.all()
    serializer_class = DatasetSerializer
    pagination_class = pagination.LimitOffsetPagination


class FromGbifView(APIView):
    """Resolve a GBIF occurrence key to resolver ID via GBIF occurrence API."""

    authentication_classes = []
    permission_classes = []

    def get(self, request, gbif_key):
        try:
            occurrence = fetch_gbif_occurrence(gbif_key)
        except (requests.exceptions.RequestException, TemporaryGbifApiError):
            return Response(
                {"detail": "GBIF lookup temporarily unavailable"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        if not occurrence:
            raise Http404("GBIF occurrence key not found")

        occurrence_id = occurrence.get("occurrenceID")
        if not occurrence_id:
            return Response(
                {"detail": "GBIF occurrence has no occurrenceID"},
                status=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )

        return HttpResponseRedirect("/{}".format(quote(occurrence_id, safe="")))
