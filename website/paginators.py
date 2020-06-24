from rest_framework.pagination import PageNumberPagination, CursorPagination, LimitOffsetPagination
from django.core.paginator import Paginator
from django.utils.functional import cached_property
from django.db import connection
from rest_framework.response import Response

class CustomPagination(LimitOffsetPagination):
    def get_count(self, queryset):
        if 'WHERE "website_darwincoreobject"."data" @> \'{}\'' in str(queryset.query): # No filters
            with connection.cursor() as cursor:
                cursor.execute("SELECT reltuples FROM pg_class WHERE relname = 'website_darwincoreobject'")
                count = cursor.fetchone()
                return int(count[0])
        return queryset.count()

