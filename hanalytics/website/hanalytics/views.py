# Create your views here.
from datetime import datetime, timedelta
from django.shortcuts import render_to_response

import pyes

def _get_stamp(timestamp):
    if timestamp < 0:
        return datetime.fromtimestamp(0) + timedelta(microseconds=timestamp*1000)
    else:
        return datetime.fromtimestamp(timestamp/1000)
def year_histogram(request):
    es = pyes.ES("localhost:9200")
    query = pyes.MatchAllQuery()
    query = query.search(size=21)
    query.facet.facets.append(pyes.facets.DateHistogramFacet(
        "date", "date", "year"
    ))
    result = es.search(query=query)
    entries = result['facets']['date']['entries']
    context = {
        "histogram": [(_get_stamp(entry['time']), entry['count']) for entry in entries]
    }
    return render_to_response("hanalytics/year-histogram.html", context)