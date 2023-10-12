from django.urls import path
from . import PatternController
from django.conf.urls import include, url

urlpatterns = [
    url(r'^$', PatternController.PatternListController.as_view(), name='patterns'),
    url(r'^parser_details$', PatternController.ParserDetailsController.as_view(), name='parser-details'),
    url(r'^(?P<grok_id>[^/]+)$', PatternController.PatternController.as_view(), name='pattern-grokid'),
    url(r'^(?P<grok_id>[^/]+)/product/(?P<product_id>[^/]+)$', PatternController.PatternProductController.as_view(), name='pattern-grokid-prodid'),
    
]