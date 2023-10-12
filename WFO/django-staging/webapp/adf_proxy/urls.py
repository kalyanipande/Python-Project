from django.urls import path,include
from . import adf_views


urlpatterns = [
    path('prometheus/', include('django_prometheus.urls')),
    path('healthcheck/',adf_views.HealthCheck.as_view(),name="healthcheck"),
    path('access_denied/',adf_views.AccessDeniendPage.as_view(),name='access_denied'),
    path('cookie_expired/',adf_views.CookieExpiredPage.as_view(),name='cookie_expired'),
    path("amqloadtest/",adf_views.AMQLoadTesting.as_view(),name="amqloadtest" ),
    path("key-cloak-logout/",adf_views.KeycloakUserLogout.as_view(),name = "key_cloak_logout"),
]