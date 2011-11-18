from django.conf.urls.defaults import *


urlpatterns = patterns("",
    url(r'^year-histogram.html', 'website.hanalytics.views.histogram', kwargs={"interval":"year"}),
    url(r'^month-histogram.html', 'website.hanalytics.views.histogram', kwargs={"interval":"month"}),
    url(r'^search.html', 'website.hanalytics.views.search')
)
