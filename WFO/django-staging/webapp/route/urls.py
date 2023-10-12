"""adf URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from django.conf.urls import include
from django.conf.urls.static import static
from django.conf import settings
from .routers import router

urlpatterns = [
    path('orch/api/', include('route.routers')),  # Django API's must be develop here
    path('orch/uam/', include('uam.urls')),
    path('orch/', include('adf_proxy.urls')),
] + static("/", document_root=settings.STATIC_ROOT)
