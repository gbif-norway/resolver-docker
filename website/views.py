from django.shortcuts import get_object_or_404, render
from django.http import HttpResponse
from .models import DarwinCoreObject

def index(request):
    return HttpResponse('Resolver')

def detail(request, uuid):
    darwin_core_object = get_object_or_404(DarwinCoreObject, uuid=uuid)
    return render(request, 'detail.html', {'darwin_core_object': darwin_core_object})
