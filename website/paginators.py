from rest_framework.pagination import PageNumberPagination, CursorPagination, LimitOffsetPagination
from django.core.paginator import Paginator
from django.utils.functional import cached_property
from django.db import connection
from rest_framework.response import Response


class CustomPagination(LimitOffsetPagination):
    def get_paginated_response(self, data):
        return Response({
            'next': self.get_next_link(),
            'previous': self.get_previous_link(),
            'results': data
        })

    def paginate_queryset(self, queryset, request, view=None):
        self.limit = self.get_limit(request)
        if self.limit is None:
            return None

        self.offset = self.get_offset(request)

        if 'search' in request.GET:
            count = 0
        else:
            with connection.cursor() as cursor:
                cursor.execute("SELECT reltuples FROM pg_class WHERE relname = 'website_darwincoreobject'")
                count = cursor.fetchone()
                count = int(count[0])
        self.count = count

        self.request = request
        if self.count > self.limit and self.template is not None:
            self.display_page_controls = True
        return list(queryset[self.offset:self.offset + self.limit])

