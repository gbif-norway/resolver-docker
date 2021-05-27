from rest_framework.pagination import LimitOffsetPagination
from populator.models import Statistic


class CustomPagination(LimitOffsetPagination):
    def get_count(self, queryset):
        no_filters = ['WHERE "website_resolvableobject"."data" @> {}',
                      'WHERE "website_resolvableobject"."data" @> \'{}\'']
        if no_filters[0] in str(queryset.query) or no_filters[1] in str(queryset.query):
            return Statistic.objects.get_total_count()
        elif 'WHERE "website_resolvableobject"."data" @> {"basisofrecord": "Preservedspecimen"}' in str(queryset.query):
            return Statistic.objects.get_preserved_specimen_count()
        return queryset.count()

