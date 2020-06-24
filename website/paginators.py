from rest_framework.pagination import LimitOffsetPagination
from django.db import connection


class CustomPagination(LimitOffsetPagination):
    def get_count(self, queryset):
        if 'WHERE "website_darwincoreobject"."data" @> \'{}\'' in str(queryset.query): # No filters
            with connection.cursor() as cursor:
                cursor.execute("SELECT reltuples FROM pg_class WHERE relname = 'website_darwincoreobject'")
                count = cursor.fetchone()
                return int(count[0])
        return queryset.count()

