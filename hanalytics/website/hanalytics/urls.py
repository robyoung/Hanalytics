from django.conf.urls.defaults import *


urlpatterns = patterns("",
    url(r'^year-histogram.html', 'website.hanalytics.views.year_histogram'),
)
