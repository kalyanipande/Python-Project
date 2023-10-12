from django.urls import path
from . import views

urlpatterns = [
    path('testapi/',views.TestAPI.as_view(),name="test"),
]