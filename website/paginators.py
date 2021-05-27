from rest_framework.pagination import LimitOffsetPagination
from rest_framework.response import Response
from populator.models import Statistic
from collections import OrderedDict


class CustomPagination(LimitOffsetPagination):
    def get_count(self, queryset):
        no_filters = ['WHERE "website_resolvableobject"."data" @> {}',
                      'WHERE "website_resolvableobject"."data" @> \'{}\'']
        if no_filters[0] in str(queryset.query) or no_filters[1] in str(queryset.query):
            return Statistic.objects.get_total_count()
        elif 'WHERE "website_resolvableobject"."data" @> {"basisofrecord": "Preservedspecimen"}' in str(queryset.query):
            return Statistic.objects.get_preserved_specimen_count()
        return 500
        #return queryset.count()

    def get_paginated_response(self, data):
        return Response(OrderedDict([
            ('next', self.get_next_link()),
            ('previous', self.get_previous_link()),
            ('results', data)
        ]))
