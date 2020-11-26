from rest_framework.pagination import LimitOffsetPagination
from django.db import connection
from populator.models import Statistic


class CustomPagination(LimitOffsetPagination):
    def get_count(self, queryset):
        no_filters = ['WHERE "website_resolvableobject"."data" @> {}',
                      'WHERE "website_resolvableobject"."data" @> \'{}\'']
        if no_filters[0] in str(queryset.query) or no_filters[1] in str(queryset.query):
            return Statistic.objects.get_total_count()
        return queryset.count()

