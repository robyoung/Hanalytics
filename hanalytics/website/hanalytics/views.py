from datetime import datetime, timedelta
from django.shortcuts import render_to_response
import pandas
import pandas.stats.moments as moments
import pytz

import pyes

def _get_stamp(timestamp):
    if timestamp < 0:
        return datetime.fromtimestamp(0, pytz.timezone("Europe/London")) + timedelta(microseconds=timestamp*1000)
    else:
        return datetime.fromtimestamp(timestamp/1000, pytz.timezone("Europe/London"))

def search(request):
    es = pyes.ES("localhost:9200")
    if request.GET.get("term"):
        query = pyes.TermQuery("text", request.GET['term'])
    else:
        query = pyes.MatchAllQuery()
    query = query.search(size=100, sort=[{"date":{"order":"desc"}}])
    result = es.search(query=query)
    return render_to_response("hanalytics/search.html", {"result":result})

def _get_histogram_data(es, term, interval):
    query = pyes.TermQuery("text", term).search()
    query.facet.facets.append(pyes.facets.DateHistogramFacet(
        "date", "date", interval
    ))
    return dict(
        (_get_stamp(entry['time']), entry['count']) for entry in es.search(query=query)['facets']['date']['entries']
    )
def histogram(request, interval="year"):
    es = pyes.ES("localhost:9200")
    roll = int(request.GET.get("roll", "0"))
    terms = request.GET.getlist("term")
    data = dict(
        (term, _get_histogram_data(es, term, interval)) for term in terms
    )
    frame = pandas.DataFrame(data).fillna(0)
    if roll:
        frame = moments.ewma(frame, roll)[roll:]

    context = {
        "histogram": frame
    }
    return render_to_response("hanalytics/%s-histogram.html" % interval, context)