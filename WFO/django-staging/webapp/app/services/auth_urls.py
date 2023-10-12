from . import auth
from django.conf.urls import include, url

urlpatterns = [
    # baseurl/orch/api/ctoken/
	url(r'^ctoken', auth.LoginUser.as_view(), name='auth_ctoken'),
]