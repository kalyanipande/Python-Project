from django.urls import path, re_path
from . import KnownSignaturesController
from django.conf.urls import include, url

urlpatterns = [
    url(r'^known_signatures$', KnownSignaturesController.AdminKnownSignatureApprovalList.as_view(), name='update_known_signature'),
    url(r'^known_signatures/(?P<known_signature_id>[^/]+)$', KnownSignaturesController.AdminKnownSignatureScopeUpdate.as_view(), name='update_known_signature'),
]