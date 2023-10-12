from django.urls import path, re_path
from . import BlacklistedSignaturesController
from django.conf.urls import include, url

urlpatterns = [
    url(r'^blacklisted_signatures$', BlacklistedSignaturesController.AdminBlacklistedSignatureApprovalList.as_view(), name='update_blacklisted_signature'),
    url(r'^blacklisted_signatures/(?P<blacklisted_signature_id>[^/]+)$', BlacklistedSignaturesController.AdminBlacklistedSignatureScopeUpdate.as_view(), name='update_blacklisted_signature'),
]