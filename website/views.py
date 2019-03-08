from django.shortcuts import render
from django.http import HttpResponse

def index(request):
    return HttpResponse('Resolver')

def detail(request, uuid):
    return HttpResponse('Details')
