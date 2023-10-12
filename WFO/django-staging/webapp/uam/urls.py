from django.urls import path
from . import views


urlpatterns = [
    path(r'check_avail', views.check_user_availability,name='check_avail'),
    path(r'check_email', views.check_email_availability,name='check_email'),
    path(r'get_region_list', views.get_region_list,name='get_region_list'),
    path('key-cloak-user/', views.KeyCloakUser.as_view(),name='key_cloak_user'),
    path("key-cloak-user/<str:id>/",views.KeyCloakUserDetail.as_view(),name='key_cloak_user_detail'),
    path("key-cloak-group/",views.KeyCloakGroup.as_view(),name='key_cloak_group'),
    path("key-cloak-user-group-map/",views.KeycloakGroupMap.as_view(),name='key_cloak_user_group_map'),
    path("key-cloak-user-group-map-delete/",views.KeycloakGroupMapDelete.as_view(),name='keycloak_usergroup_map_delete'),
    path("events/",views.GetEvents.as_view(),name='events'),
    path("key-cloak-logout/",views.KeycloakUserLogout.as_view(),name='key_cloak_logout'),
    # path("key-cloak-un-authenticate-user",views.UnAuthecateUser.as_view(),name='key_cloak_unauthorisied_user'),
]
