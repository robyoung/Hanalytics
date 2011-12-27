from django.shortcuts import render_to_response
from django.template.context import RequestContext
import pymongo

def viewer(request):
    col = pymongo.Connection()['hanalytics']['speech']
    speeches = col.find(sort=[("_id", pymongo.DESCENDING)], limit=20)
    return render_to_response("hansard/viewer.html", RequestContext(request, {"speeches":speeches}))