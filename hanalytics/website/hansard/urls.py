from django.conf.urls.defaults import *


urlpatterns = patterns("",
    url(r'^viewer.html', 'website.hansard.views.viewer'),
)
