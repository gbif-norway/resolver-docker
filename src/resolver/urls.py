"""resolver URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import include, path
from website import views
from rest_framework import routers
from django.conf import settings

router = routers.SimpleRouter()
router.register(r'datasets', views.DatasetViewSet)
router.register(r'history', views.HistoryViewSet)
# Registered last: its detail route matches any path ending in '/', because ids can contain '/'
router.register(r'', views.ResolvableObjectViewSet)

urlpatterns = [
        path('healthz/', views.healthz, name='healthz'),
        path('readyz/', views.readyz, name='readyz'),
        path('from-gbif/<str:gbif_key>', views.FromGbifView.as_view(), name='from-gbif'),
        path('from-gbif/<str:gbif_key>/', views.FromGbifView.as_view()),
    ]

if settings.DEBUG:
    import debug_toolbar
    urlpatterns.append(path('__debug__/', include(debug_toolbar.urls)))

# Must stay last so the resolvable object detail route does not shadow the patterns above.
# The flip side is that ids like 'datasets/<x>', 'history/<x>', 'healthz' or 'readyz' resolve to those routes instead
urlpatterns.append(path('', include(router.urls)))
