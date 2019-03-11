from django.shortcuts import render
from django.http import HttpResponse
from .models import DarwinCoreObject

def index(request):
    return HttpResponse('Resolver')

def detail(request, uuid):
    return render(request, 'detail.html', {'darwin_core_object': DarwinCoreObject.objects.get(uuid=uuid)})
