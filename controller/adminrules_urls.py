from django.urls import path
from . import RulesController
from django.conf.urls import include, url

urlpatterns = [
    url(r'^$', RulesController.RulesListController.as_view(), name='rules'),
    url(r'^rule_details$', RulesController.RuleDetailsController.as_view(), name='rule-details'),
    url(r'^(?P<rule_id>[^/]+)$', RulesController.RulesController.as_view(), name='rule-ruleid'),
]